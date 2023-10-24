from logging import Logger
from typing import List

from sprint.stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection
from airflow.models.variable import Variable
from pendulum import parse

import requests
from datetime import date, datetime
import json

class Deliveries_API_Repository:
    def list_deliveries(self, last_date:str,today:date,yestrday:date) -> List[dict]:
        objc = []
        last_date=str(last_date)
   
        if yestrday > last_date:

            deliveries_threshold = 0
            limit = 50

            while True:

                api_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries' 
                headers = { 
                        'X-Nickname': Variable.get('X-Nickname'), 
                        'X-Cohort': Variable.get('X-Cohort'), 
                        'X-API-KEY': Variable.get('X-API-KEY')
                }

                params = {
                    'from': yestrday,
                    'to': today,
                    'sort_field': '_id',
                    'sort_direction': 'asc',
                    'limit': limit,
                    'offset': deliveries_threshold
                }

    
                response = requests.get(api_url, headers=headers, params=params)
                response.raise_for_status() 
                data = response.json()
                if not data:  
                    break
                objc.extend(data)
                deliveries_threshold += limit
            

            return objc


class DeliveriesDestRepository:

    def insert_deliveries(self, conn: Connection, deliveries: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliveries(object_value, update_ts)
                    VALUES (%(object_value)s, %(update_ts)s);
                """,
                {
                    "object_value": json.dumps(deliveries,ensure_ascii=False),
                    "update_ts": datetime.today().strftime('%Y-%m-%d')
                },
            )


class DeliveriesLoader:
    WF_KEY = "project_deliveries_origin_to_stg_workflow"
    LOAD_DATE = "last_load_date"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = DeliveriesDestRepository()
        self.api=Deliveries_API_Repository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self,today,yesterday):
        
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)

            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LOAD_DATE: datetime(2020,1,1)})

            load_date = wf_setting.workflow_settings[self.LOAD_DATE]
            
            load_queue = self.api.list_deliveries(load_date, today, yesterday)

            if not load_queue:
                self.log.info("Quitting.")
                return
            
            self.log.info(f"Found {len(load_queue)} deliveries to load.")

            for deliveries in load_queue:
                deliveries_decoded = json.loads(json.dumps(deliveries))
                self.stg.insert_deliveries(conn, deliveries_decoded)

            wf_setting.workflow_settings[self.LOAD_DATE] = yesterday
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LOAD_DATE]}")