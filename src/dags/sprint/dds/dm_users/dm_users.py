import logging

import pendulum
from airflow.decorators import dag, task
from sprint.dds.dm_users.dm_users_loader import dm_users_loader
from sprint import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'sprint'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_sprint_dds_dm_users_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_dm_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = dm_users_loader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_users()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_users_dict = load_dm_users()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_users_dict  # type: ignore


dds_dm_users_dag = sprint5_sprint_dds_dm_users_dag()