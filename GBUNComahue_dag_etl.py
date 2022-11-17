"""
Story 5
## Grupo de Universidades B
## UNComahue

COMO: Analista de datos
QUIERO: Implementar el Python Operator para Transformación
PARA: procesar los datos obtenidos de la base de datos dentro del DAG

Criterios de aceptación: 
Configurar el Python Operator para que ejecute las dos funciones que procese 
los datos para las siguientes universidades:
- Univ. Nacional Del Comahue
- Universidad Del Salvador

Documentación
https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/python/index.html#module-airflow.operators.python"

# Dev: Aldo Agunin
# Fecha: 11/11/2022
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import logging
import logging.config
import pandas as pd
from plugins.callables import csv_a_txt

# ------- DECLARACIONES -----------
universidad_corto = 'UNComahue'
universidad_largo = 'Universidad Nacional del Comahue'

# ------- LOGGER ------------------
log_cfg = ('GB' + universidad_corto + '_log.cfg')
configfile = Path(__file__).parent.parent / 'plugins' / log_cfg
logging.config.fileConfig(configfile, disable_existing_loggers=False)
logger = logging.getLogger(__name__)

#---------------  extraccion  --------------
def datos_a_csv():
    logger.info('*** Comenzando Extracción ***')
    ## Ubicacion del .sql
    sql_path = Path(__file__).parent.parent / 'include' / ('GB' + universidad_corto + '.sql')
    ## Leo el .sql
    sql_consulta = open(sql_path, 'r').read()
    ## Conexion a la base
    hook = PostgresHook(postgres_conn_id='alkemy_db')
    conexion = hook.get_conn()
    df = pd.read_sql(sql_consulta, conexion)
    ## Guardo .csv
    csv_path = Path.cwd() / 'files' / ('GB' + universidad_corto + '_select.csv')
    df.to_csv(csv_path, index=False)
    logger.info('*** Fin Extraccion ***')
    return
#------------------------------------------

dag = DAG(
    dag_id=f'GB{universidad_corto}_dag_etl',   # dag_id='GBUNComahue_dag_etl',
    description=f'DAG para hacer ETL de la {universidad_largo}',
    tags=['Aldo', 'ETL'],
    start_date=datetime(2022, 11, 1),
    schedule=timedelta(hours=1),  # cada hora
    catchup=False,
    default_args={
        'retries': 5, # If a task fails, it will retry 5 times.
        'retry_delay': timedelta(minutes=5),
        },
    )

# primera tarea: correr script .SQL y la salida a .CSV
# se usara un PythonOperator que ejecute la consulta .SQL de la Story1 y genere salida a .CSV
# extract = EmptyOperator(
#     task_id="extraction_task",
#     dag=dag
#     )
extract = PythonOperator(
        task_id='extraction_task',
        python_callable=datos_a_csv,
        dag=dag,
    )

# segunda tarea: procesar datos en pandas
# se usara un PythonOperator que llame a un modulo externo
# transform = EmptyOperator(
#     task_id="transformation_task",
#     dag=dag
#     )
transform = PythonOperator(
        task_id='transformation_task',
        python_callable=csv_a_txt,
        dag=dag,
    )

# tercera tarea: subir resultados a amazon s3
# se usara un operador desarrollado por la comunidad
load = EmptyOperator(
    task_id="load_task",
    dag=dag
    )

extract >> transform >> load
