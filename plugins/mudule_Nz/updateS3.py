from airflow.providers.amazon.aws.hooks.s3 import S3Hook
S3_CONN_ID = 'astro-s3-GF'
BUCKET = 'astro-GF'
#name = 'covid_data'  # swap your name here

target_file = r'/usr/local/airflow/include/GFUMoron.txt'

def upload_to_s3(file,endpoint, date):
 
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

    s3_hook.load_string(file.text, '{0}_{1}.csv'.format(endpoint, date), bucket_name=BUCKET, replace=True)