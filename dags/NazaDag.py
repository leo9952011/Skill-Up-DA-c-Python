from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging

import pandas as pd
from decouple import config as settings
from sqlalchemy import create_engine
from typing import List





#############################LOGER#############################

def create_logger():  

    logger = logging.getLogger('models_logger') 
    logger.setLevel(logging.INFO) 
    logfile = logging.FileHandler(r'dags/logs/log_file.log') 
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt) 
    logfile.setFormatter(formatter) 
    logger.addHandler(logfile) 
      
    return logger 

log = create_logger()

#############################FUNCTIONS#############################

##########SQL EXECUTE##########
SQLALCHEMY_DATABASE_URL = f"postgresql://{settings('DB_USERNAME')}:{settings('DB_PASSWORD')}@{settings('DB_HOST')}/{settings('DB_NAME')}"
engine = create_engine(SQLALCHEMY_DATABASE_URL)

def get_query(path:List):
    for i in path:
        try:
            with open(i, 'r',encoding="utf-8") as myfile:
                data = myfile.read()
                data.replace('\n', ' ')
                log.info(f"query in process {i} ")
                return str(data)
        except:
            log.warning(f"SyntaxError: invalid syntax for query {i} ")
            pass



def sql_execute():
    log.info('INIT sql EXECUTE')
    
    path_rio_Cuarto = get_query([r'include\GFUNRioCuarto.sql'])
    path_Moron = get_query([r'include\GFUMoron.sql'])
    
    log.info('conection to database')
    try:
        df_mr = pd.read_sql(path_Moron,con = engine)
        log.info('available dataframe U-Moron')
        
        df_mr.to_csv(r'files/GFUMoron.csv')
        log.info('available csv U-Moron')
        
    except:
        log.warning(f'Error in {path_Moron}')
        
        
    try:
        df_rc = pd.read_sql(path_rio_Cuarto,con = engine)
        log.info('available dataframe U-Rio-Cuarto')
        
        df_rc.to_csv(r'files/GFUNRioCuarto.csv')
        log.info('available csv U-Rio-Cuarto')
        
    except:
        log.warning(f'Error in {path_rio_Cuarto}')
        


def data_processing():
    log.info('procesamiento en pandas')
def data_upload():
    log.info('carga de datos a S3')



#############################DAG#############################

default_args = {
    'owner':'Nazareno',
    'depends_on_past':False,
    'email':'airflow@example.com',
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':5,
    'retry_delay': timedelta(minutes=1)
}



with DAG(
    'Naza-Dag',
    default_args=default_args,
    description='Naza Test DAG',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    tags=['test','dag','naza']
) as dag:
    sql_task = PythonOperator(task_id ='SQL-EXECUTE', python_callable=sql_execute)
    processing_task = PythonOperator(task_id ='PANDAS', python_callable=data_processing)
    upload_task = PythonOperator(task_id ='SQL', python_callable=data_upload)
    
    sql_task >> processing_task >> upload_task
 