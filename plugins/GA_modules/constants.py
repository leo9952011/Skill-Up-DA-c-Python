import os

WORKING_DIR = os.getcwd()
DAGS_DIR = os.path.join(WORKING_DIR, 'dags')
DYN_DAGS_DIR = os.path.join(DAGS_DIR, 'dynamic_dags')
DATASETS_DIR = os.path.join(WORKING_DIR, 'datasets')
FILES_DIR = os.path.join(WORKING_DIR, 'files')
INCLUDE_DIR = os.path.join(WORKING_DIR, 'include')
LOGS_DIR = os.path.join(WORKING_DIR, 'mylogs')

LOGGER_CFG_PATH = os.path.join(WORKING_DIR, 'logger.cfg')
POSTAL_DATA_PATH = os.path.join(WORKING_DIR, 'assets', 'codigos_postales.csv')

POSTGRES_CONN_ID = 'alkemy_db'
S3_CONN_ID = 'astro-s3-workshop'
S3_BUCKET = 'astro-workshop-bucket'
