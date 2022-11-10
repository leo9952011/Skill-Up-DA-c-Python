"""ETL DAG for 'Facultad Latinoamericana de Ciencias Sociales' (Grupo G)."""

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_task():
    """Get FLCienciasSociales data from remote postgres DB and save to csv file locally."""

    local_basepath = Path(__file__).resolve().parent.parent

    # Read SQL query
    sql_filepath = local_basepath / 'include/GGFLCienciasSociales.sql'
    with open(sql_filepath, encoding='utf8') as sql_file:
        sql_str = sql_file.read()

    # connect to remote postgres DB via hook
    pg_hook = PostgresHook(postgres_conn_id="alkemy_db")
    uni_df = pg_hook.get_pandas_df(sql=sql_str)

    # save df data to csv locally
    csv_filepath = local_basepath / 'files/GGFLCienciasSociales_select.csv'
    uni_df.to_csv(csv_filepath, sep=',', header=True, encoding='utf-8')


def transform_task():
    """Load data from local csv, normalize with pandas and save to txt file locally."""

    print(f'Transform all data task placeholder...')


with DAG('GGFLCienciasSociales_dag_etl',
        start_date=datetime(2022,11,1),
        catchup=False,
        schedule_interval='@hourly',
        ) as dag:

    extract = PythonOperator(task_id='extract',
                             python_callable=extract_task,
                             retries=5)

    transform = PythonOperator(task_id='transform',
                             python_callable=transform_task,
                             retries=5)

    # Load FLACSO data from local txt to S3. Uses LocalFilesystemToS3Operator Operator.
    load = EmptyOperator(task_id='load',
                         retries=5)

    # set task dependencies
    transform >> extract >> load
