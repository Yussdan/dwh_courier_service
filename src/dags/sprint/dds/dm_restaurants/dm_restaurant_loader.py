from logging import Logger
from typing import List

from sprint.dds.dds_settings_repository  import EtlSetting, DdsEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

class dm_restaurant_obj(BaseModel):
    id:int
    restaurant_id: str
    restaurant_name: str
    active_from:datetime
    active_to:datetime


class dm_restaurant_originrepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_restaurant(self, load_dm_restaurant_threshold: int, limit:int) -> List[dm_restaurant_obj]:
        with self._db.client().cursor(row_factory=class_row(dm_restaurant_obj)) as cur:
            cur.execute(
                """
                    select 
                        id,
                        replace(((object_value::json)->'_id')::text,'"','') as restaurant_id,
                        replace(((object_value::json)->'name') ::text,'"','') as restaurant_name,
                        update_ts as active_from,
                        '2099-12-31' ::timestamp as active_to
                    from stg.ordersystem_restaurants ou
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": load_dm_restaurant_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class dm_restaurant_destRepository:

    def insert_dm_restaurant(self, conn: Connection, load_dm_restaurant: dm_restaurant_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id,restaurant_name,active_from,active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s,%(active_to)s);
                """,
                {
                    "restaurant_id":load_dm_restaurant.restaurant_id,
                    "restaurant_name": load_dm_restaurant.restaurant_name,
                    "active_from": load_dm_restaurant.active_from,
                    "active_to": load_dm_restaurant.active_to,
                },
            )


class dm_restaurant_loader:
    WF_KEY = "sprint_load_dm_restaurant_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dm_restaurant_originrepository(pg_origin)
        self.stg = dm_restaurant_destRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_restaurant(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_restaurant(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_restaurant to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_restaurant in load_queue:
                self.stg.insert_dm_restaurant(conn, dm_restaurant)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")