import logging

import pendulum
from airflow.decorators import dag, task
from project.stg.deliveries.deliveries_loader import DeliveriesLoader
from sprint import ConnectionBuilder

log = logging.getLogger(__name__)

# Инициализация дага
@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_stg_deliveries_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")


    # Объявляем таск, который загружает данные.
    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DeliveriesLoader(dwh_pg_connect, log)
        rest_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    couriers_dict = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    couriers_dict  # type: ignore


stg_bonus_system_deliveries_dag = sprint5_project_stg_deliveries_dag()


	


# Операторы dag





