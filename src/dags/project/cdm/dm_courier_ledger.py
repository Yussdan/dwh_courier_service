import logging

import pendulum
from airflow.decorators import dag, task
from project.cdm.dm_courier_ledger_loader import dsl_loader
from sprint import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_dsl__dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dsl__load")
    def load_dsl_():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = dsl_loader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dsl()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_orders_dict = load_dsl_()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_orders_dict  # type: ignore


dsl__dag = sprint5_project_dsl__dag()