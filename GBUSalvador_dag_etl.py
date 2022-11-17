"""
Story 7
## Grupo de Universidades B
## USalvador

COMO: Analista de datos
QUIERO: Utilizar un operador creado por la comunidad
PARA: poder subir el txt creado por el operador de Python al S3

Criterios de aceptación: 
- Tomar el .txt del repositorio base 
- Buscar un operador creado por la comunidad que se adecue a los datos.
- Configurar el S3 Operator para la Univ. del Salvador
- Subir el archivo a S3

# Dev: Aldo Agunin
# Fecha: 15/11/2022
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
universidad_corto = 'USalvador'
universidad_largo = 'Universidad del Salvador'

#root_path = Path(__file__).parent.parent
root_path = Path.cwd()
## Ubicacion de .sql
sql_path = root_path / 'include'
sql_file = sql_path / ('GB' + universidad_corto + '.sql')
## Ubicacion de .csv
csv_path = root_path / 'files'
csv_file = csv_path / ('GB' + universidad_corto + '_select.csv')
## Ubicación de.txt
txt_path = root_path / 'datasets'
txt_file = txt_path / ('GB' + universidad_corto + '_process.txt')

# ------- LOGGER ------------------
def configure_logger():
    logger_name = 'GB' + universidad_corto + '_dag_etl'
    logger_cfg = Path.cwd() / 'plugins' / 'GB_logger.cfg'
    logging.config.fileConfig(logger_cfg)
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
    sql_file = sql_path / ('GB' + universidad_corto + '.sql')
    ## Ubicacion de .csv
    csv_path = root_path / 'files'
    csv_file = csv_path / ('GB' + universidad_corto + '_select.csv')

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
    
    csv_a_txt(univ=universidad_corto, in_file=csv_file, out_file=txt_file)
    
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
	#root_path = Path(__file__).parent.parent
	#root_path = Path.cwd()
	#logger.info(local_basepath)
    # Upload to S3
    txt_path = local_basepath / 'datasets/GBUSalvador_process.txt'
	## Ubicación de.txt
    #txt_path = root_path / 'datasets'
    #txt_name = ('GB' + universidad_corto + '_process.txt')
    #txt_file = txt_path / txt_name
    #logger.info(txt_file)
    s3_hook.load_file(txt_file,
        bucket_name=bucket_name,
        replace=True,
        key='process/GBUSalvador_process.txt')

    logger.info('*** Fin Load  ***')


with DAG(
    dag_id=f'GB{universidad_corto}_dag_etl',   # dag_id='GBUNComahue_dag_etl',
    description=f'DAG para hacer ETL de la {universidad_largo}',
    tags=['Aldo', 'ETL'],
    start_date=datetime(2022, 11, 1),
    schedule=timedelta(hours=1),  # cada hora
    catchup=False,
    default_args={
        'retries': 5, # If a task fails, it will retry 5 times.
        'retry_delay': timedelta(minutes=5),
        }, ) as dag:

    # primera tarea: correr script .SQL y la salida a .CSV
    # se usara un PythonOperator que ejecute la consulta .SQL de la Story1 y genere salida a .CSV
    # extract = EmptyOperator(
    #     task_id="Extract",
    #     dag=dag
    #     )
    extract = PythonOperator(task_id='Extract', python_callable=extract_task)

    # segunda tarea: procesar datos en pandas
    # se usara un PythonOperator que llame a un modulo externo
    # transform = EmptyOperator(
    #     task_id="Transform",
    #     dag=dag
    #     )
    transform = PythonOperator(task_id='Transform', python_callable=transform_task)

    # tercera tarea: subir resultados a amazon s3
    # se usara un operador desarrollado por la comunidad
    # load = EmptyOperator(
    #     task_id="Load",
    #     dag=dag
    #     )
    load = PythonOperator(task_id='Load', python_callable=load_task)

    extract >> transform >> load
    
