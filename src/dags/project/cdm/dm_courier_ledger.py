import logging

import pendulum
from airflow.decorators import dag, task
from project.cdm.dm_courier_ledger_loader import dsl_loader
from sprint import ConnectionBuilder
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import date

log = logging.getLogger(__name__)


@dag(
    schedule_interval='@daily',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  
    catchup=False,  
    tags=['sprint5', 'cdm', 'project'],  
    is_paused_upon_creation=True,
)
def sprint5_project_cdm__dag():

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dsl__load")
    def load_dsl_(yesterday):

        rest_loader = dsl_loader(dwh_pg_connect, log)
        rest_loader.load_dsl(yesterday) 

    load_dsl_ = load_dsl_('{{yesterday_ds}}')

    wait_dm = ExternalTaskSensor(
        task_id='wait_for_dm_deliveries',
        external_dag_id='sprint5_project_dds_dag',
        check_existence=True,
        execution_delta=pendulum.Duration(days=0)
    )

    wait_dm>>load_dsl_  

dsl__dag = sprint5_project_cdm__dag()