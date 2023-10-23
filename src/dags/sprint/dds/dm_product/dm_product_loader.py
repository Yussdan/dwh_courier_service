from logging import Logger
from typing import List

from sprint.dds.dds_settings_repository  import EtlSetting, DdsEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

class dm_product_obj(BaseModel):
    id:int
    restaurant_id:int
    product_id:str
    product_name:str
    product_price:float
    active_from:datetime
    active_to:datetime


class dm_product_originrepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_product(self, load_dm_product_threshold: int, limit:int) -> List[dm_product_obj]:
        with self._db.client().cursor(row_factory=class_row(dm_product_obj)) as cur:
            cur.execute(
                """
                    with product as
                    (
                        select id,(object_value::jsonb)->>'_id' as res, jsonb_array_elements(((object_value::jsonb)->>'menu')::jsonb)as men,update_ts 
                        from stg.ordersystem_restaurants or2
                    )
                    select 
                        product.id,
                        dr.id as restaurant_id , 
                        replace(((men)->'_id')::text,'"','') as product_id,
                        replace(((men)->'name')::text,'"','') as product_name,
                        (men->'price') as product_price,
                        update_ts as active_from,
                        '2099-12-31'::timestamp as active_to
                    from product
                        inner join dds.dm_restaurants dr on dr.restaurant_id=product.res 
                    WHERE dr.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY dr.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": load_dm_product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class dm_product_destRepository:

    def insert_dm_product(self, conn: Connection, load_dm_product: dm_product_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id,product_id,product_name,product_price,active_from,active_to)
                    VALUES (%(restaurant_id)s,%(product_id)s, %(product_name)s,%(product_price)s,%(active_from)s,%(active_to)s);
                """,
                {
                    "restaurant_id":load_dm_product.restaurant_id,
                    "product_id":load_dm_product.product_id,
                    "product_name": load_dm_product.product_name,
                    "product_price":load_dm_product.product_price,
                    "active_from": load_dm_product.active_from,
                    "active_to": load_dm_product.active_to,
                },
            )


class dm_product_loader:
    WF_KEY = "sprint_load_dm_product_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dm_product_originrepository(pg_origin)
        self.stg = dm_product_destRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_product(self):
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
            load_queue = self.origin.list_dm_product(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_product to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_product in load_queue:
                self.stg.insert_dm_product(conn, dm_product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")