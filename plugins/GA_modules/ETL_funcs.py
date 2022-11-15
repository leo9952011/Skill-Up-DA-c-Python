import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.GA_modules.constants import LOGGER_CFG_PATH, POSTGRES_CONN_ID
from plugins.GA_modules.logger import logger_func



def extract_func(
    university_id,
    sql_path,
    csv_path,
    logger,
    db_conn_id=POSTGRES_CONN_ID,
):
    """Read query statement from .sql file in INCLUDE_DIR, get data from
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
    """

    # Read sql query statement from .sql in INCLUDE_DIR
    with open(sql_path, 'r') as f:
        command = f.read()

    # Get hook, load data into pandas df and save it as .csv in FILES_DIR
    pg_hook = PostgresHook.get_hook(db_conn_id)
    df = pg_hook.get_pandas_df(command)
    df.to_csv(csv_path, index=False)

    logger.info(
        f'Extraction done: read {university_id}.sql in INCLUDE_DIR and '
        f'created {university_id}_select.csv in FILES_DIR')
