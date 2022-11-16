from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_to_s3(file,name):
    S3_CONN_ID = 'aws_s3_bucket'
    BUCKET = 'astrogf'
    with open(file) as fl:
        txt = fl.read()
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.load_string(txt,f'preprocess/{name}.txt', bucket_name=BUCKET, replace=True)
    
