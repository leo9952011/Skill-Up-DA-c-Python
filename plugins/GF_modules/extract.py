import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import TypeVar
from pathlib import Path




PathLike = TypeVar("PathLike", str, Path, None)

def std_extract(sql_path: PathLike,csv_path : PathLike):
    
    df = False
    logging.info('preparing sql execution')
    try:
        with open(sql_path, 'r') as myfile:
            sql_query = myfile.read()
        hook = PostgresHook(postgres_conn_id='alkemy_db')
        df = hook.get_pandas_df(sql = sql_query)      
        df.to_csv(csv_path)
        logging.info('successful sql execution')
        df = True
    except:
        logging.warning('sql execution failed')
        pass
    return df


