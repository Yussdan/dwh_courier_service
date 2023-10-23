from logging import Logger
from typing import List

from sprint.cdm.cdm_settings_repository  import EtlSetting, DdsEtlSettingsRepository
from sprint import PgConnect
import json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import date

class dsr_obj(BaseModel):
    restaurant_id:str
    restaurant_name:str
    settlement_date:date
    orders_count:int
    orders_total_sum:float
    orders_bonus_payment_sum:float
    orders_bonus_granted_sum:float
    order_processing_fee:float
    restaurant_reward_sum:float
    row_number:int
    

class dsr_originrepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dsr(self, load_dsr_threshold: int,limit:int) -> List[dsr_obj]:
        with self._db.client().cursor(row_factory=class_row(dsr_obj)) as cur:
            cur.execute(
                """
                        with table_for_count as
                        (
                            select 
                            restaurant_id,
                            restaurant_name,
                            count(*) as orders_count,
                            settlement_date
                            from
                            (
                                SELECT 
                                    distinct
                                    dr.restaurant_id,
                                    restaurant_name,
                                    order_key,
                                    date AS settlement_date
                                FROM dds.dm_orders do2
                                INNER JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id
                                INNER JOIN dds.fct_product_sales fps ON fps.order_id = do2.id
                                INNER JOIN dds.dm_restaurants dr ON do2.restaurant_id = dr.id
                                WHERE order_status = 'CLOSED'
                            ) ab
                            group by restaurant_id,restaurant_name,settlement_date
                        )
                        select  
                            sub.restaurant_id,
                            sub.restaurant_name,
                            sub.settlement_date,
                            orders_count,
                            orders_total_sum,
                            orders_bonus_payment_sum,
                            orders_bonus_granted_sum,
                            order_processing_fee,
                            restaurant_reward_sum,
                            row_number
                        from
                            (
                            SELECT 
                                distinct
                                dr.restaurant_id,
                                restaurant_name,
                                date AS settlement_date,
                                sum(total_sum) AS orders_total_sum,
                                sum(bonus_payment) AS orders_bonus_payment_sum,
                                sum(bonus_grant) AS orders_bonus_granted_sum,
                                sum(total_sum) * 0.25 AS order_processing_fee,
                                sum(total_sum) - sum(bonus_payment) - sum(total_sum) * 0.25 AS restaurant_reward_sum,
                                ROW_NUMBER() OVER (ORDER BY date, dr.restaurant_id) AS row_number
                            FROM dds.dm_orders do2
                            INNER JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id
                            INNER JOIN dds.fct_product_sales fps ON fps.order_id = do2.id
                            INNER JOIN dds.dm_restaurants dr ON do2.restaurant_id = dr.id
                            WHERE order_status = 'CLOSED'
                            GROUP BY dr.restaurant_id, restaurant_name, date
                        ) sub
                            inner join table_for_count tfc on tfc.restaurant_id=sub.restaurant_id and sub.settlement_date=tfc.settlement_date  
                    where row_number>%(threshold)s
                    order by sub.settlement_date
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": load_dsr_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class dsr_destRepository:

    def insert_dsr(self, conn: Connection, load_dsr: dsr_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(restaurant_id,restaurant_name,settlement_date,orders_count,orders_total_sum,orders_bonus_payment_sum,orders_bonus_granted_sum,order_processing_fee,restaurant_reward_sum)
                    VALUES (%(restaurant_id)s,%(restaurant_name)s, %(settlement_date)s,%(orders_count)s,%(orders_total_sum)s,%(orders_bonus_payment_sum)s,%(orders_bonus_granted_sum)s,%(order_processing_fee)s,%(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id,settlement_date) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id":load_dsr.restaurant_id,
                    "restaurant_name":load_dsr.restaurant_name,
                    "settlement_date": load_dsr.settlement_date,
                    "orders_count":load_dsr.orders_count,
                    "orders_total_sum":load_dsr.orders_total_sum,
                    "orders_bonus_payment_sum": load_dsr.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum":load_dsr.orders_bonus_granted_sum,
                    "order_processing_fee": load_dsr.order_processing_fee,
                    "restaurant_reward_sum":load_dsr.restaurant_reward_sum,
                },
            )


class dsr_loader:
    WF_KEY = "sprint_load_dsr_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dsr_originrepository(pg_origin)
        self.stg = dsr_destRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dsr(self):
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
            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dsr(last_loaded_id, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dsr to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dsr in load_queue:
                self.stg.insert_dsr(conn, dsr)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.row_number for t in load_queue])

            wf_setting_json = json.dumps(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


