import logging
from datetime import timedelta
from airflow.utils.dates import days_ago

def create_logger():  

    logger = logging.getLogger('Dag_logger') 
    logger.setLevel(logging.INFO) 
    logfile = logging.FileHandler(r'/usr/local/airflow/files/log_file.log') 
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt) 
    logfile.setFormatter(formatter) 
    logger.addHandler(logfile) 
      
    return logger 

log = create_logger()

from airflow.decorators import (
    dag, 
    task
    )

@dag(
    schedule=timedelta(seconds=15),
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "retries": 5,
    },
    tags=['example'])
def dagNz():
    @task()
    def data():
        log.info("SQL Query")
        with open(r'/usr/local/airflow/include/GFUNRioCuarto.sql', 'r') as myfile:
            sql_query = myfile.read()
            print(sql_query)
            
        
    @task()
    def printwarning():
        log.warning("log de warning")
    
    @task()
    def printcritical():
        log.critical("log error critico")
        
    data()
    printwarning()
    printcritical()

dagNz = dagNz()
