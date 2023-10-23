from logging import Logger
from typing import List

from sprint.dds.dds_settings_repository  import EtlSetting, DdsEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class fct_product_sales_obj(BaseModel):
    id:int
    product_id:int
    order_id:int
    count:int
    price:int
    deliveries_id : int
    rate : int
    tip_sum : int
    courier_id : int
    total_sum:int
    bonus_payment:int
    bonus_grant:int

class fct_product_sales_originrepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fct_product_sales(self, load_fct_product_sales_threshold: int, limit:int) -> List[fct_product_sales_obj]:
        with self._db.client().cursor(row_factory=class_row(fct_product_sales_obj)) as cur:
            cur.execute(
                """
                    with order_product_fact as 
                    (
                        select 
                            id,
                            object_id as order_id,
                            jsonb_array_elements(((object_value::jsonb)->>'order_items')::jsonb)->'quantity' as count,
                            replace((jsonb_array_elements(((object_value::jsonb)->>'order_items')::jsonb)->'id')::text,'"','') as product_id
                        from stg.ordersystem_orders oo 
                    ),
                    bonus_pay as 
                    (
                    select 
                        replace(((be.event_value::jsonb)->'order_id')::text,'"','') as order_id,
                        jsonb_array_elements(((be.event_value::jsonb)->'product_payments')::jsonb)->>'product_id' as product_id,
                        jsonb_array_elements(((be.event_value::jsonb)->'product_payments')::jsonb)->>'bonus_payment' as bonus_payment ,
                        jsonb_array_elements(((be.event_value::jsonb)->'product_payments')::jsonb)->>'bonus_grant' as bonus_grant
                    from stg.bonussystem_events be 
                    )
                    select 
                        orf.id,
                        dp.id as product_id,
                        do2.id as order_id,
                        count, 
                        do2.deliveries_id,
                        dp.product_price as price,
                        dp.product_price * count::int  as total_sum,
                        dd.rate,
                        dd.tip_sum,
                        dd.courier_id,
                        bp.bonus_payment::numeric(14,2),
                        bp.bonus_grant::numeric(14,2)
                    from order_product_fact orf
                        inner join dds.dm_orders do2 on orf.order_id=do2.order_key 
                        inner join dds.dm_products dp on orf.product_id=dp.product_id
                        inner join bonus_pay bp on bp.order_id=orf.order_id and orf.product_id=bp.product_id
                        inner join dds.dm_deliveries dd on dd.id=do2.deliveries_id 
                    WHERE orf.id > %(threshold)s and do2.order_status='CLOSED' --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY orf.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": load_fct_product_sales_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class fct_product_sales_destRepository:

    def insert_fct_product_sales(self, conn: Connection, load_fct_product_sales: fct_product_sales_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id,order_id, count, price,total_sum,bonus_payment,bonus_grant,deliveries_id,rate,courier_id,tip_sum)
                    VALUES (%(product_id)s,%(order_id)s, %(count)s, %(price)s,%(total_sum)s,%(bonus_payment)s,%(bonus_grant)s,%(deliveries_id)s,%(rate)s,%(courier_id)s,%(tip_sum)s);
                """,
                {
                    "product_id": load_fct_product_sales.product_id,
                    "order_id":load_fct_product_sales.order_id,
                    "count":load_fct_product_sales.count,
                    "price":load_fct_product_sales.price,
                    "total_sum":load_fct_product_sales.total_sum,
                    "bonus_payment":load_fct_product_sales.bonus_payment,
                    "bonus_grant":load_fct_product_sales.bonus_grant,
                    "deliveries_id":load_fct_product_sales.deliveries_id,
                    "rate":load_fct_product_sales.rate,
                    "courier_id":load_fct_product_sales.courier_id,
                    "tip_sum":load_fct_product_sales.tip_sum,

                },
            )


class fct_product_sales_loader:
    WF_KEY = "sprint_load_fct_product_sales_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = fct_product_sales_originrepository(pg_origin)
        self.stg = fct_product_sales_destRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fct_product_sales(self):
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
            load_queue = self.origin.list_fct_product_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fct_product_sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fct_product_sales in load_queue:
                self.stg.insert_fct_product_sales(conn, fct_product_sales)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")