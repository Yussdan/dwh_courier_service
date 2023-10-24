import logging

import pendulum
from airflow.decorators import dag, task
from project.stg.stg_couries.couriers_loader import CouriersLoader
from sprint import ConnectionBuilder
from project.stg.deliveries.deliveries_loader import DeliveriesLoader
from sprint import ConnectionBuilder
log = logging.getLogger(__name__)

# Инициализация дага
@dag(
    schedule_interval= "@daily", 
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  
    catchup=False,  
    tags=['sprint5', 'stg', 'project'],  
    is_paused_upon_creation=True  
)
def sprint5_project_stg_dag():

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="couries_load")
    
    def load_couriers():

        rest_loader = CouriersLoader(dwh_pg_connect, log)
        rest_loader.load_couriers()  
    
    @task(task_id="deliveries_load")
    def load_deliveries(current_ds, cur_yesterday_ds): 
        rest_loader = DeliveriesLoader(dwh_pg_connect, log)
        rest_loader.load_deliveries(today=current_ds, yesterday=cur_yesterday_ds)

    deliveries_dict = load_deliveries('{{ds}}', '{{yesterday_ds}}') 

    deliveries_dict  

    couriers_dict = load_couriers()

    couriers_dict  
    
stg_dag = sprint5_project_stg_dag()