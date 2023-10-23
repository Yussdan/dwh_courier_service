from logging import Logger
from typing import List

from sprint.dds.dds_settings_repository  import EtlSetting, DdsEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class dm_orders_obj(BaseModel):
        id:int
        user_id:int
        restaurant_id:int
        timestamp_id:int
        order_key:str
        deliveries_id : int
        order_status:str


class dm_orders_originrepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_orders(self, load_dm_orders_threshold: int, limit:int) -> List[dm_orders_obj]:
        with self._db.client().cursor(row_factory=class_row(dm_orders_obj)) as cur:
            cur.execute(
                """
                    select 
                        oo.id,
                        du.id as user_id,
                        dr.id as restaurant_id,
                        dt.id as timestamp_id,
                        dd.id as deliveries_id ,
                        object_id as order_key , 
                        replace(((object_value::json)->'final_status')::text,'"','') as order_status
                    from stg.ordersystem_orders oo 
                        inner join dds.dm_timestamps dt on ((((object_value::json)->'date')::text)::timestamp)=dt.ts 
                        inner join dds.dm_restaurants dr on replace(((((object_value::json)->'restaurant')::json)->'id')::text,'"','')=dr.restaurant_id 
                        inner join dds.dm_users du on replace(((((object_value::json)->'user')::json)->'id')::text,'"','')=du.user_id 
                        inner join dds.dm_deliveries dd on oo.id=dd.order_id 
                    WHERE oo.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY oo.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": load_dm_orders_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class dm_orders_destRepository:

    def insert_dm_orders(self, conn: Connection, load_dm_orders: dm_orders_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id,restaurant_id,timestamp_id,order_key,order_status,deliveries_id)
                    VALUES (%(user_id)s,%(restaurant_id)s, %(timestamp_id)s,%(order_key)s,%(order_status)s,%(deliveries_id)s);
                """,
                {
                    "user_id":load_dm_orders.user_id,
                    "restaurant_id":load_dm_orders.restaurant_id,
                    "deliveries_id":load_dm_orders.deliveries_id,
                    "timestamp_id": load_dm_orders.timestamp_id,
                    "order_key":load_dm_orders.order_key,
                    "order_status": load_dm_orders.order_status,
                },
            )


class dm_orders_loader:
    WF_KEY = "sprint_load_dm_orders_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dm_orders_originrepository(pg_origin)
        self.stg = dm_orders_destRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_orders(self):
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
            load_queue = self.origin.list_dm_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_orders in load_queue:
                self.stg.insert_dm_orders(conn, dm_orders)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


