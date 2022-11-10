from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator


# Constants
UNIVERSITY_ID = 'GAUFlores'


with DAG(
    dag_id=f'{UNIVERSITY_ID}_dag_etl',
    description=f'ETL process for {UNIVERSITY_ID}',
    schedule_interval='@hourly',
    start_date=datetime(2022, 11, 9),
    default_args={'retries': 5, 'retry_delay': timedelta(minutes=5)}
) as dag:
    # Read .sql in /include, save data as .csv in /files.  Use PythonOp-
    # erator
    extract = EmptyOperator(task_id='extract_func')

    # Apply requested formats for each column, add location or postal
    # code from codigos_postales.csv in /assets, compute ages from
    # birthdate, eliminate wrong age registries and save data as .txt in
    # /datasets.  Use PythonOperator.
    transform = EmptyOperator(task_id='transform_func')

    # Load .txt from /datasets, upload to S3.  Use PythonOperator.
    load = EmptyOperator(task_id='load_func')

    extract >> transform >> load
