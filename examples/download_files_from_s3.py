import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET_NAME = 'your-s3-bucket-name'
LOCAL_PATH = '/your/local/dst'

def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook('ariflow_s3_connect_id')
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name

def download_directory_from_s3(key_prefix: str, bucket_name: str, local_path: str):
    hook = S3Hook('ariflow_s3_connect_id')
    
    # List all keys (objects) under the directory in the bucket
    keys = hook.list_keys(bucket_name=bucket_name, prefix=key_prefix)
    
    if not keys:
        raise Exception(f"No objects found for key_prefix: {key_prefix} in bucket: {bucket_name}")
        
    downloaded_files = []
    for key in keys:
        file_name = key.split('/')[-1]
        local_file_path = os.path.join( local_path, file_name)
        
        downloaded_filename = hook.download_file(key=key, bucket_name=bucket_name, local_path= local_path)
        os.rename(src= downloaded_filename, dst= local_file_path)
        downloaded_files.append(local_path)

    return downloaded_files

    
with DAG(
    dag_id='s3_download',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

    # Download a file
    task_download_from_s3 = PythonOperator(
        task_id='download_directory_from_s3',
        python_callable=download_directory_from_s3,
        op_kwargs={
            'key_prefix': 'bucket_subdirectory/',
            'bucket_name': BUCKET_NAME,
            'local_path': LOCAL_PATH
        }
    )    