import pandas as pd
from datetime import datetime

from airflow.decorators import dag, task 
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sqlpath = "/usr/local/airflow/include/GCUPalermo.sql"
csvpath = "/usr/local/airflow/files"

@dag(
    'palermo_dag',
    description = 'Dag para ETL de la Universidad de Palermo',
    schedule_interval = "@hourly",
    start_date = datetime(2022, 11, 4)
)
def palermo_dag():
    @task(task_id='extract_palermo', retries = 5)
    def get_data_palermo(**kwargs):

        with open(sqlpath, 'r') as sqlfile:
            select_query = sqlfile.read()
        hook = PostgresHook(postgres_conn_id='alkemy_db')
        df = hook.get_pandas_df(sql=select_query)
        df.to_csv(f'{csvpath}/GCUPalermo_select.csv')
    
    task_2 = DummyOperator(task_id='task_2', retries = 5) #PythonOperator(task_id=transform_palermo, retries = 5, python_callable=transform_palermo)
    task_3 = DummyOperator(task_id='task_3', retries = 5) #PythonOperator(task_id=load, retries = 5, python_callable=s3_load)

    get_data_palermo() >>task_2 >> task_3

dag = palermo_dag()