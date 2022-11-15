from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd

from plugins.GA_modules.constants import FILES_DIR, INCLUDE_DIR
from plugins.GA_modules.ETL_funcs import extract_func



# Constants
UNIVERSITY_ID = 'GAUFlores'
SQL_PATH = os.path.join(INCLUDE_DIR, f'{UNIVERSITY_ID}.sql')
CSV_PATH = os.path.join(FILES_DIR, f'{UNIVERSITY_ID}_select.csv')


with DAG(
    dag_id=f'{UNIVERSITY_ID}_dag_etl',
    description=f'ETL process for {UNIVERSITY_ID}',
    schedule_interval='@hourly',
    start_date=datetime(2022, 11, 15)
) as dag:
    # Read .sql in INCLUDE_DIR, save data as .csv in FILES_DIR
    extract = PythonOperator(
        task_id='extract_func',
        python_callable=extract_func,
        op_kwargs={
            'university_id': UNIVERSITY_ID,
            'sql_path': SQL_PATH,
            'csv_path': CSV_PATH,
        },
        retries=5
    )

    # Apply requested formats for each column, add location or postal
    # code from codigos_postales.csv in ASSETS_DIR, compute ages from
    # birthdate, eliminate wrong age registries and save data as .txt in
    # DATASETS_DIR.  Use PythonOperator.
    transform = EmptyOperator(task_id='transform_func')

    # Load .txt from DATASETS_DIR, upload to S3.  Use PythonOperator.
    load = EmptyOperator(task_id='load_func')

    extract >> transform >> load
