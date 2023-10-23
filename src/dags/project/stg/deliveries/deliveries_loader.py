from logging import Logger
from typing import List

from sprint.stg import EtlSetting, StgEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection

import requests
from datetime import datetime
import json

class Deliveries_API_Repository:
    def list_deliveries(self, deliveries_threshold: int, limit:int) -> List[dict]:
        
        api_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries'
        headers = {
                'X-Nickname': 'yussden',
                'X-Cohort': '17',
                'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
            }
        params = {
                'sort_field': '_id',
                'sort_direction': 'asc',
                'limit': limit,
                'offset': deliveries_threshold
            }
        response = requests.get(api_url, headers=headers, params=params)
        objs = response.json()
        return objs


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
    COUNT_ROWS = "count_added_rows"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = DeliveriesDestRepository()
        self.api=Deliveries_API_Repository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.COUNT_ROWS: 0})

            # Вычитываем очередную пачку объектов.
            offset= wf_setting.workflow_settings[self.COUNT_ROWS]
            load_queue = self.api.list_deliveries(offset, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for deliveries in load_queue:
                deliveries_decoded = json.loads(json.dumps(deliveries))
                self.stg.insert_deliveries(conn, deliveries_decoded)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            previous_count = wf_setting.workflow_settings[self.COUNT_ROWS]
            current_count = len(load_queue)
            wf_setting.workflow_settings[self.COUNT_ROWS] = previous_count + current_count
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.COUNT_ROWS]}")