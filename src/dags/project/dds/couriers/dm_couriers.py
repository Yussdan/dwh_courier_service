import logging

import pendulum
from airflow.decorators import dag, task
from project.dds.couriers.dm_couriers_loader import dm_couriers_loader
from sprint import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_dds_dm_couriers_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = dm_couriers_loader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_couriers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_couriers_dict = load_dm_couriers()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_couriers_dict  # type: ignore


dds_dm_couriers_dag = sprint5_project_dds_dm_couriers_dag()