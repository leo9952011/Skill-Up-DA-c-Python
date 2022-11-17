"""
S8
COMO: Analista de datos
QUIERO: Arreglar un Dag dinamico
PARA: Poder ejecutarlos normalmente

Criterios de aceptacion: 
- El DAG a arreglar es el que procesa las siguientes universidades:
    - Univ. Nacional Del Comahue
    - Universidad Del Salvador
- Se debe arreglar el DAG factory
- No se debe tocar la logica de procesamiento o negocio"

"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pathlib import Path
import logging
import logging.config
import pandas as pd
#from plugins.GBfunciones import csv_a_txt
from plugins.GBtransform import csv_a_txt

# ------- DECLARACIONES -----------
universidad_corto = 'UNComahue'
universidad_largo = 'Universidad Nacional del Comahue'

#root_path = Path(__file__).parent.parent
root_path = Path.cwd()
## Ubicacion de .sql
sql_path = root_path / 'include'
sql_file = sql_path / ('GBUNComahue.sql')
## Ubicacion de .csv
csv_path = root_path / 'files'
csv_file = csv_path / ('GBUNComahue_select.csv')
## Ubicacion de.txt
txt_path = root_path / 'datasets'
txt_file = txt_path / ('GBUNComahue_process.txt')

# ------- LOGGER ------------------
def configure_logger():
    logger_name = 'GBUNComahue_dag_etl'
    logger_cfg = Path.cwd() / 'plugins' / 'GB_logger.cfg'
    logging.config.fileConfig(logger_cfg, disable_existing_loggers=False)
    # Set up logger
    logger = logging.getLogger(logger_name)
    return logger

# ---------------  extraccion  --------------
def extract_task():
    """ extrae datos de la base training y los guarda en un archivo csv """
    logger = configure_logger()
    logger.info('*** Comenzando Extraccion ***')

    root_path = Path(__file__).parent.parent
    ## Ubicacion de .sql
    sql_path = root_path / 'include'
    sql_file = sql_path / ('GBUNComahue.sql')
    ## Ubicacion de .csv
    csv_path = root_path / 'files'
    csv_file = csv_path / ('GBUNComahue_select.csv')

    ## Leo el .sql
    sql_consulta = open(sql_file, 'r').read()
    ## Conexion a la base
    hook = PostgresHook(postgres_conn_id='alkemy_db')
    conexion = hook.get_conn()
    df = pd.read_sql(sql_consulta, conexion)
    ## Guardo .csv
    df.to_csv(csv_file, index=False)
    
    logger.info('*** Fin Extraccion ***')
    return

# ---------------  transformacion  --------------
def transform_task():
    """ transforma los datos del csv y los guarda en un archivo txt """
    logger = configure_logger()
    logger.info('*** Comenzando Transformacion ***')
    
    csv_a_txt(univ='UNComahue', in_file=csv_file, out_file=txt_file)    
    logger.info('*** Fin Transformacion ***')
    return

#---------------- carga ----------------------
def load_task():
    """ carga el txt en un el bucket S3 """

    logger = configure_logger()
    logger.info('*** Comenzando Load ***')
    
    s3_hook = S3Hook(aws_conn_id='aws_s3_bucket')
    bucket_name = 'alkemy-gb'
    local_basepath = Path(__file__).resolve().parent.parent
    # Upload to S3
    txt_path = local_basepath / 'datasets/GBUNComahue_process.txt'
    s3_hook.load_file(txt_file,
        bucket_name=bucket_name,
        replace=True,
        key='process/GBUNComahue_process.txt')

    logger.info('*** Fin Load ***')

with DAG('GBUNComahue_dag_etl',
        start_date=datetime(2022,11,1),
        catchup=False,
        schedule_interval='@hourly',
        ) as dag:

    extract = PythonOperator(task_id='extract',
                             python_callable=extract_task)

    transform = PythonOperator(task_id='transform',
                             python_callable=transform_task)

    load = PythonOperator(task_id='load',
                             python_callable=load_task)

    extract >> transform >> load