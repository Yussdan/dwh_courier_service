from logging import Logger
from typing import List

from sprint.stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection
import requests
from datetime import datetime
import json
from airflow.models.variable import Variable

class Couriers_API_Repository:
    def list_couriers(self, couriers_threshold: int, limit:int) ->  List[dict]:
        
        api_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
        headers = {
                'X-Nickname': Variable.get("X-Nickname"),
                'X-Cohort': Variable.get("X-Cohort"),
                'X-API-KEY': Variable.get("X-API-KEY")
            }
        params = {
                'sort_field': '_id',
                'sort_direction': 'asc',
                'limit': limit,
                'offset': couriers_threshold
            }
        response = requests.get(api_url, headers=headers, params=params)
        objs = response.json()
        return objs


class CouriersDestRepository:

    def insert_couriers(self, conn: Connection, couriers: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.couriers(object_value, update_ts)
                    VALUES (%(object_value)s, %(update_ts)s);
                """,
                {
                    "object_value": json.dumps(couriers, ensure_ascii=False),
                    "update_ts": datetime.today().strftime('%Y-%m-%d')
                },
            )

class CouriersLoader:
    WF_KEY = "project_couriers_origin_to_stg_workflow"
    COUNT_ROWS = "count_added_rows"
    BATCH_LIMIT = 50 

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = CouriersDestRepository()
        self.api=Couriers_API_Repository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):

        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.COUNT_ROWS: 0})


            offset= wf_setting.workflow_settings[self.COUNT_ROWS]
            load_queue = self.api.list_couriers(offset, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for couriers in load_queue:
                couriers_decoded = json.loads(json.dumps(couriers)) 
                self.stg.insert_couriers(conn, couriers_decoded)

            previous_count = wf_setting.workflow_settings[self.COUNT_ROWS]
            current_count = len(load_queue)
            wf_setting.workflow_settings[self.COUNT_ROWS] = previous_count + current_count
            wf_setting_json = json2str(wf_setting.workflow_settings) 
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.COUNT_ROWS]}")