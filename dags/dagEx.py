import logging
from datetime import timedelta

from airflow.utils.dates import days_ago
from airflow.decorators import (dag, task)
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from typing import List

@dag(
    schedule=timedelta(minutes=60),
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "retries": 5,
    },
    tags=['DataProcessing'])
def DataProcessing():
    
    @task()
    def dataRc():
        
        df = False
        logging.info('preparing sql execution')
        try:
            with open(r'/usr/local/airflow/include/GFUNRioCuarto.sql', 'r') as myfile:
                sql_query = myfile.read()
            print(sql_query)
            hook = PostgresHook(postgres_conn_id='pgconnectionAlkemy')
            df = hook.get_pandas_df(sql = sql_query)
            print(df.head())
            print(type(df))       
            df.to_csv(r'/usr/local/airflow/include/GFUNRioCuarto.csv')
            logging.info('successful sql execution U.N.RIO-CUARTO')
        except:
            logging.warning('sql execution failed U.N.RIO-CUART')
            pass
        return df
     
    
    @task()
    def dataMr():
        
        df = False
        logging.info('preparing sql execution')
        try:
            with open(r'/usr/local/airflow/include/GFUMoron.sql', 'r') as myfile:
                sql_query = myfile.read()
            print(sql_query)
            hook = PostgresHook(postgres_conn_id='pgconnectionAlkemy')
            df = hook.get_pandas_df(sql = sql_query)
            print(df.head())
            print(type(df))    
            df.to_csv(r'/usr/local/airflow/include/GFUMoron.csv')
            df = pd.read_csv(r'/usr/local/airflow/include/GFUMoron.csv') 
            logging.info('veamos si existe veamos si existeveamos si existeveamos si existeveamos si existeveamos si existe')
            print(df.head())       
            logging.info('successful sql execution U.N.RIO-CUARTO')
        except:
            logging.warning('sql execution failed U.N.RIO-CUART')
            pass
        return df
        
        
        
    @task()
    def transformation(dfr:List):
        for i in dfr:
            if i:
                logging.info('Pandas process')
            else:
                logging.warning("log de warning")
    
    @task()
    def printcritical():
        logging.critical("log error critico")
        
    dfr = [dataRc(), dataMr()]
    transformation(dfr)
    printcritical()

DataProcessingDag = DataProcessing()
