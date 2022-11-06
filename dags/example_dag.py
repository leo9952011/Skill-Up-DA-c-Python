import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.baseoperator import chain # A function that sets sequential dependencies between tasks including lists of tasks.
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.weekday import BranchDayOfWeekOperator


import logging


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def extract():
    """
    #### Extract task
    A simple "extract" task to get data ready for the rest of the
    pipeline. In this case, getting data is simulated by reading from a
    hardcoded JSON string.
    """
    logging.info("----------------- Extract -------------------")

def transform():
    """
    #### Transform task
    A simple "transform" task which takes in the collection of order data and
    computes the total order value.
    """
    logging.info("----------------- Transform -------------------")

def load():
    """
    #### Load task
    A simple "load" task that takes in the result of the "transform" task and prints it out,
    instead of saving it to end user review
    """
    logging.info("----------------- Load -------------------")

@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule_interval="@hourly",
    # This DAG is set to run for the first time on January 1, 2021. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    start_date=datetime(2022, 11, 10),
    # When catchup=False, your DAG will only run for the latest schedule_interval. In this case, this means
    # that tasks will not be run between January 1, 2021 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the schedule_interval
    catchup=False,
    default_args=default_args,
    tags=[
        'Universidad Tecnológica Nacional',
        'Universidad Nacional De Tres De Febrero'
    ]) # If set, this tag is shown in the DAG view of the Airflow UI
def example_dag():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using three simple tasks for extract, transform, and load.
    For more information on Airflow's TaskFlow API, reference documentation here:
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
    """

    begin = DummyOperator(task_id="begin")
    # Last task will only trigger if no previous task failed
    end = DummyOperator(task_id="end")
    
    extract_task = PythonOperator(python_callable=extract, task_id="scrape")
    transform_task = PythonOperator(python_callable=transform, task_id="process")
    load_task = PythonOperator(python_callable=load, task_id="save")

    # High-level dependencies between tasks
    chain(begin, extract_task, transform_task, load_task, end)

example_dag = example_dag()