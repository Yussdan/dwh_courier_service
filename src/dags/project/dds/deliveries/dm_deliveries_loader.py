from logging import Logger
from typing import List

from sprint.dds.dds_settings_repository  import EtlSetting, DdsEtlSettingsRepository
from sprint import PgConnect
from sprint.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class deliveries_obj(BaseModel):
    order_id : int
    timestamp_id : int
    delivery_id : str
    courier_id : int
    rate : int
    tip_sum : float


class dm_deliveries_origin_repository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, load_deliveries_threshold: int, limit:int) -> List[deliveries_obj]:
        with self._db.client().cursor(row_factory=class_row(deliveries_obj)) as cur:
            cur.execute(
                """
                    with i as 
                    (
                        select 
                            replace(((object_value::JSON)->'order_id')::text,'"','') as order_id ,
                            replace(((object_value::JSON)->'courier_id')::text,'"','') as courier_id,
                            replace(((object_value::JSON)->'delivery_id')::text,'"','') as delivery_id,
                            replace(((object_value::JSON)->'rate')::text,'"','') as rate,
                            replace(((object_value::JSON)->'tip_sum')::text,'"','') as tip_sum
                        from stg.deliveries d 
                    )
                    select 
                        do2.id as order_id,
                        do2.timestamp_id as timestamp_id ,
                        i.delivery_id as delivery_id,
                        dc.id as courier_id ,
                        i.rate as rate,
                        i.tip_sum as tip_sum
                    from i
                        inner join dds.dm_couriers dc on dc.courier_id=i.courier_id
                        inner join dds.dm_orders do2 on do2.order_key=i.order_id
                    WHERE do2.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY do2.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": load_deliveries_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class dm_deliveries_Repository:

    def insert_deliveries(self, conn: Connection, load_deliveries: deliveries_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(order_id,timestamp_id,delivery_id,courier_id,rate,tip_sum)
                    VALUES (%(order_id)s,%(timestamp_id)s,%(delivery_id)s,%(courier_id)s,%(rate)s,%(tip_sum)s);
                """,
                {
                    "courier_id":load_deliveries.courier_id,
                    "timestamp_id":load_deliveries.timestamp_id,
                    "order_id":load_deliveries.order_id,
                    "delivery_id":load_deliveries.delivery_id,
                    "rate":load_deliveries.rate,
                    "tip_sum":load_deliveries.tip_sum
                },
            )


class dm_deliveries_loader:
    WF_KEY = "project_load_deliveries_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dm_deliveries_origin_repository(pg_origin)
        self.stg = dm_deliveries_Repository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_deliveries(self):
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
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for deliveries in load_queue:
                self.stg.insert_deliveries(conn, deliveries)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.order_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


