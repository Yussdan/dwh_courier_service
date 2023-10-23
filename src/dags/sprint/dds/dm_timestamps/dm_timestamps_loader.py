from logging import Logger
from typing import List

from sprint.dds.dds_settings_repository  import EtlSetting, DdsEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, time,date

class dm_timestamps_obj(BaseModel):
    id:int
    ts:datetime
    year: int
    month: int
    day:int
    time:time
    date:date


class dm_timestamps_originrepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_timestamps(self, load_dm_timestamps_threshold: datetime, limit:int) -> List[dm_timestamps_obj]:
        with self._db.client().cursor(row_factory=class_row(dm_timestamps_obj)) as cur:
            cur.execute(
                """
                    select 
                        id,
                        (((object_value::json)->'date')::text)::timestamp as ts,
                        extract(year from (((object_value::json)->'date')::text)::timestamp)::int as year,
                        extract(month from (((object_value::json)->'date')::text)::timestamp)::int as month,
                        extract(day from (((object_value::json)->'date')::text)::timestamp)::int as day,
                        (((object_value::json)->'date')::text)::time as time,
                        (((object_value::json)->'date')::text)::date
                    from stg.ordersystem_orders
                    WHERE (((object_value::json)->'date')::text)::timestamp  > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY ts ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": load_dm_timestamps_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class dm_timestamps_destRepository:

    def insert_dm_timestamps(self, conn: Connection, load_dm_timestamps: dm_timestamps_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts,year,month,day,time,date)
                    VALUES (%(ts)s, %(year)s, %(month)s,%(day)s,%(time)s,%(date)s);
                """,
                {
                    "ts":load_dm_timestamps.ts,
                    "year": load_dm_timestamps.year,
                    "month": load_dm_timestamps.month,
                    "day": load_dm_timestamps.day,
                    "time": load_dm_timestamps.time,
                    "date": load_dm_timestamps.date,
                },
            )


class dm_timestamps_loader:
    WF_KEY = "sprint_load_dm_timestamps_to_dds_workflow"
    LAST_LOADED_ts_KEY = "last_loaded_ts"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dm_timestamps_originrepository(pg_origin)
        self.stg = dm_timestamps_destRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_timestamps(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ts_KEY: '2020-01-01'})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ts_KEY]
            load_queue = self.origin.list_dm_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_timestamps in load_queue:
                self.stg.insert_dm_timestamps(conn, dm_timestamps)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ts_KEY] = max([t.ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ts_KEY]}")