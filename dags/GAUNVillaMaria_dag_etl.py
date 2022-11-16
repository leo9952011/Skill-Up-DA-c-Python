from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

from plugins.constants import DATASETS_DIR, FILES_DIR, INCLUDE_DIR, \
    POSTGRES_CONN_ID


# Constants
UNIVERSITY_ID = 'GAUNVillaMaria'
SQL_PATH = os.path.join(INCLUDE_DIR, f'{UNIVERSITY_ID}.sql')
CSV_PATH = os.path.join(FILES_DIR, f'{UNIVERSITY_ID}_select.csv')


def extract_func(
    university_id=UNIVERSITY_ID,
    sql_path=SQL_PATH,
    csv_path=CSV_PATH,
    db_conn_id=POSTGRES_CONN_ID
):
    '''Read query statement from .sql file in INCLUDE_DIR, get data from
    postgres db and save it as .csv in FILES_DIR

    Parameters
    ----------
    university_id: string
        Unique per-university identifier to set names of files, custom-
        ize logging information, etc.  Default is local constant UNIVERS
        ITY_ID.
    sql_path: string or path object
        System path to the .sql file containing the sql query statement.
        Default is local constant SQL_PATH.
    csv_path: string or path object
        System path with location and name of the .csv file to be creat-
        ed.  Default is local constant CSV_PATH.
    db_conn_id: string
        Db connection id as configured in Airflow UI (Admin -> Connect-
        ions).  Default is plugins.constants.POSTGRES_CONN_ID.

    Returns
    -------
    None.
    '''

    # Read sql query statement from .sql in INCLUDE_DIR
    with open(sql_path, 'r') as f:
        command = f.read()

    # Get hook, load data into pandas df and save it as .csv in FILES_DIR
    pg_hook = PostgresHook.get_hook(db_conn_id)
    df = pg_hook.get_pandas_df(command)
    df.to_csv(csv_path, index=False)


with DAG(
    dag_id=f'{UNIVERSITY_ID}_dag_etl',
    description=f'ETL process for {UNIVERSITY_ID}',
    schedule_interval='@hourly',
    start_date=datetime(2022, 11, 11),
    default_args={'retries': 5, 'retry_delay': timedelta(minutes=5)}
) as dag:
    # Read .sql in INCLUDE_DIR, save data as .csv in FILES_DIR
    extract = PythonOperator(
        task_id='extract_func',
        python_callable=extract_func,
    )

    # Apply requested formats for each column, add location or postal
    # code from codigos_postales.csv in ASSETS_DIR, compute ages from
    # birthdate, eliminate wrong age registries and save data as .txt in
    # DATASETS_DIR.  Use PythonOperator.
    transform = EmptyOperator(task_id='transform_func')

    # Load .txt from DATASETS_DIR, upload to S3.  Use PythonOperator.
    load = EmptyOperator(task_id='load_func')

    extract >> transform >> load
