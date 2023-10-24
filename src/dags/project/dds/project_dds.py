import logging

import pendulum
from airflow.decorators import dag, task
from project.dds.deliveries.dm_deliveries_loader import dm_deliveries_loader
from sprint import ConnectionBuilder
from project.dds.couriers.dm_couriers_loader import dm_couriers_loader
from airflow.sensors.external_task_sensor import ExternalTaskSensor

log = logging.getLogger(__name__)

@dag(
    schedule_interval='@daily',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  
    catchup=False,  
    tags=['sprint5', 'dds', 'project'],  
    is_paused_upon_creation=True,
)
def sprint5_project_dds_dag():

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries(yesterday):

        rest_loader = dm_deliveries_loader(dwh_pg_connect, log)
        rest_loader.load_dm_deliveries(yesterday)  

    @task(task_id="dm_couriers_load")
    def load_dm_couriers():

        rest_loader = dm_couriers_loader(dwh_pg_connect, log)
        rest_loader.load_dm_couriers() 

    wait_dm = ExternalTaskSensor(
        task_id='wait_for_dm_deliveries',
        external_dag_id='sprint5_project_dds_dag',
        check_existence=True,
        execution_delta=pendulum.Duration(days=0)
    )

    dm_deliveries_dict = load_dm_deliveries('{{yesterday_ds}}')

    dm_deliveries_dict  

    dm_couriers_dict = load_dm_couriers()

    
    wait_dm>>[dm_deliveries_dict,dm_couriers_dict]

dds_dm_deliveries_dag = sprint5_project_dds_dag()