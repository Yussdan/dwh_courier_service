from logging import Logger
from typing import List

from sprint.cdm.cdm_settings_repository  import EtlSetting, DdsEtlSettingsRepository
from sprint import PgConnect
import json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import date

class dsl_obj(BaseModel):
    courier_id : int
    courier_name : str
    settlement_year : int
    settlement_month : int
    orders_count: int
    orders_total_sum : float
    rate_avg : float
    order_processing_fee : float
    courier_order_sum : float
    courier_tips_sum : float
    courier_reward_sum : float
    

class dsl_originrepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dsl(self, pref_month:int, pref_year:int,yesterday:date) -> List[dsl_obj]:
        with self._db.client().cursor(row_factory=class_row(dsl_obj)) as cur:
            cur.execute(
                """
                    with count_order as 
                    (
                        select 
                            COUNT(distinct order_id) as month_count_order , 
                            dt."month" as mon,
                            dt."year"  as ye
                        from dds.fct_product_sales fps 
                            inner join dds.dm_orders do2 on do2.id =fps.order_id 
                            inner join dds.dm_timestamps dt on fps.order_id=do2.id 
                        group by dt."month" ,dt."year" 
                    ) 
                    , order_sum as 
                    (
                        SELECT
                            SUM(total_sum) as orders_total_sum 
                        from dds.fct_product_sales
                    )
                    , couriers_rate as 
                    (
                        select
                            dd.courier_id,
                            AVG(dd.rate) as avg_rate,
                            SUM(dd.tip_sum) as courier_tips_sum,
                            SUM(total_sum) as courier_order_sum
                        from dds.dm_deliveries dd 
                            inner join dds.dm_orders do4 on dd.order_id=do4.id 
                            inner join dds.fct_product_sales fps2 on fps2.deliveries_id=dd.id 
                        group by dd.courier_id 
                    )
                    select 
                        distinct
                        dd.courier_id as courier_id,
                        courier_name,
                        dt."year" as settlement_year ,
                        dt."month" as settlement_month,
                        month_count_order as orders_count,
                        orders_total_sum,
                        avg_rate as rate_avg,
                        orders_total_sum*0.25 as order_processing_fee,
                        CASE 
                            WHEN avg_rate < 4 THEN 
                                CASE 
                                    WHEN courier_order_sum * 0.05 < 100 THEN 100
                                    ELSE courier_order_sum * 0.05
                                END
                            WHEN avg_rate >= 4 AND avg_rate < 4.5 THEN 
                                CASE 
                                    WHEN courier_order_sum * 0.07 < 150 THEN 150
                                    ELSE courier_order_sum * 0.07
                                END
                            WHEN avg_rate >= 4.5 AND avg_rate < 4.9 THEN 
                                CASE 
                                    WHEN courier_order_sum * 0.08 < 175 THEN 175
                                    ELSE courier_order_sum * 0.08
                                END
                            ELSE 
                                CASE 
                                    WHEN courier_order_sum * 0.1 < 200 THEN 200
                                    ELSE courier_order_sum * 0.1
                                END
                        END AS courier_order_sum,
                        courier_tips_sum,
                        courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
                    from dds.fct_product_sales fps 
                        inner join dds.dm_couriers dc on dc.id=fps.courier_id 
                        inner join dds.dm_deliveries dd on dd.order_id=fps.order_id 
                        inner join dds.dm_timestamps dt on dt.id=dd.timestamp_id 
                        inner join count_order on count_order.mon=dt."month" and count_order.ye=dt."year" 
                        cross join order_sum
                        inner join couriers_rate on couriers_rate.courier_id=dc.id  
                    where dt.month=EXTRACT(MONTH FROM %(yesterday)s::date) and dt.year=EXTRACT(YEAR FROM %(yesterday)s::date) AND (dt.month>%(pref_month)s OR dt.year>%(pref_year)s)
                    order by dd.courier_id
                """,{
                    'pref_month':pref_month,
                    'pref_year':pref_year,
                    'yesterday':yesterday
                }
            )
            objs = cur.fetchall()
        return objs


class dsl_destRepository:

    def insert_dsl(self, conn: Connection, load_dsl: dsl_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id,courier_name,settlement_year,settlement_month,orders_count,orders_total_sum,rate_avg,order_processing_fee,courier_order_sum,courier_tips_sum,courier_reward_sum)
                    VALUES (%(courier_id)s,%(courier_name)s, %(settlement_year)s,%(settlement_month)s,%(orders_count)s,%(orders_total_sum)s,%(rate_avg)s,%(order_processing_fee)s,%(courier_order_sum)s,%(courier_tips_sum)s,%(courier_reward_sum)s)
                """,
                {
                    "courier_id":load_dsl.courier_id,
                    "courier_name":load_dsl.courier_name,
                    "settlement_year": load_dsl.settlement_year,
                    "settlement_month":load_dsl.settlement_month,
                    "orders_count":load_dsl.orders_count,
                    "orders_total_sum": load_dsl.orders_total_sum,
                    "rate_avg":load_dsl.rate_avg,
                    "order_processing_fee": load_dsl.order_processing_fee,
                    "courier_order_sum":load_dsl.courier_order_sum,
                    "courier_tips_sum": load_dsl.courier_tips_sum,
                    "courier_reward_sum":load_dsl.courier_reward_sum,
                },
            )


class dsl_loader:
    WF_KEY = "project_load_dsl_to_cdm_workflow"
    LAST_LOADED_MONTH = "last_loaded_month"
    LAST_LOADED_YEAR = 'last_loaded_year'

    def __init__(self,  pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dwh = dsl_originrepository(pg_dest)
        self.cdm = dsl_destRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dsl(self,yesterday):

        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_MONTH: 1,self.LAST_LOADED_YEAR:2020 })

            last_loaded_month = wf_setting.workflow_settings[self.LAST_LOADED_MONTH]
            last_loaded_year=wf_setting.workflow_settings[self.LAST_LOADED_YEAR]
            load_queue = self.dwh.list_dsl(last_loaded_month,last_loaded_year,yesterday)
            self.log.info(f"Found {len(load_queue)} dsl to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for dsl in load_queue:
                self.cdm.insert_dsl(conn, dsl)
            
            wf_setting.workflow_settings[self.LAST_LOADED_MONTH] = ([t.settlement_month for t in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_YEAR] = ([t.settlement_year for t in load_queue])

            wf_setting_json = json.dumps(wf_setting.workflow_settings) 
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_MONTH],wf_setting.workflow_settings[self.LAST_LOADED_YEAR]}")


