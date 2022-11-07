import logging
from datetime import timedelta
from airflow.utils.dates import days_ago

def create_logger():  

    logger = logging.getLogger('Dag_logger') 
    logger.setLevel(logging.INFO) 
    logfile = logging.FileHandler(r'C:\Users\Usuario\Desktop\Alk-Project-Py\Skill-Up-DA-c-Python\dags\logs\log_file.log') 
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
    def printinfo():
        log.info("log de info")
        print('hello logger')
    @task()
    def printwarning():
        log.warning("log de warning")
    
    @task()
    def printcritical():
        log.critical("log error critico")
        
    printinfo()
    printwarning()
    printcritical()

dagNz = dagNz()
