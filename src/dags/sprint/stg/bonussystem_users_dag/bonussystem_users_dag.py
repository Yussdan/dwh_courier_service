import logging
import pendulum
from airflow.decorators import dag, task
from sprint.stg.bonussystem_users_dag.user_loader import UserLoader
from sprint import ConnectionBuilder
log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_sprint_stg_bonus_system_user_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="user_load")
    def load_user():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_user()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    user_dict = load_user()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    user_dict  # type: ignore


stg_bonus_system_user_dag = sprint5_sprint_stg_bonus_system_user_dag()