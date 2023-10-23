from logging import Logger
from typing import List

from sprint.dds.dds_settings_repository  import EtlSetting, DdsEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class couriers_obj(BaseModel):
    id: int
    courier_id : str
    courier_name: str


class dm_couriers_origin_repository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, load_couriers_threshold: int, limit:int) -> List[couriers_obj]:
        with self._db.client().cursor(row_factory=class_row(couriers_obj)) as cur:
            cur.execute(
                """
                    select 
                        id,
                        replace(((object_value::JSON)->'_id')::text,'"','') as courier_id ,
                        replace(((object_value::JSON)->'name')::text,'"','') as courier_name
                    from stg.couriers c 
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": load_couriers_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class dm_couriers_Repository:

    def insert_couriers(self, conn: Connection, load_couriers: couriers_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id,courier_name)
                    VALUES (%(courier_id)s,%(courier_name)s);
                """,
                {
                    "courier_id":load_couriers.courier_id,
                    "courier_name":load_couriers.courier_name
                },
            )


class dm_couriers_loader:
    WF_KEY = "project_load_couriers_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dm_couriers_origin_repository(pg_origin)
        self.stg = dm_couriers_Repository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_couriers(self):
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
            load_queue = self.origin.list_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for couriers in load_queue:
                self.stg.insert_couriers(conn, couriers)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


