from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import boto3
import pandas as pd
import hashlib
import os
import json
import requests
import logging

default_args = {
    'owner': 'zc_250',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 21),
    'email': ['kingkazara@gmail.com'], 
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'dataset_processing_pipeline',
    default_args=default_args,
    description='A DAG for dataset retrieval, validation, and upload',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['zc_250', 'dataset_processing'],
)

# initialize S3 and RDS clients
def initialize_clients():
    s3_client = boto3.client('s3', region_name='us-east-2')
    rds_client = boto3.client('rds', region_name='us-east-2')
    return {'s3': s3_client, 'rds': rds_client}

# function to check user authentication
def authenticate_user(**kwargs):
    """
    Authenticate user based on token or credentials
    Returns True if authentication is successful, False otherwise
    """
    ti = kwargs['ti']
    user_token = kwargs['dag_run'].conf.get('user_token', None)
    user_id = kwargs['dag_run'].conf.get('user_id', None)
    
    if not user_token or not user_id:
        logging.error("Missing authentication credentials")
        return False
    
    ti.xcom_push(key='user_id', value=user_id)
    return True

# function to check if dataset exists in metadata
def check_dataset_existence(**kwargs):
    """
    Check if the dataset already exists in metadata tables
    Returns True if dataset exists, False otherwise
    """
    ti = kwargs['ti']
    dataset_name = kwargs['dag_run'].conf.get('dataset_name')
    file_name = kwargs['dag_run'].conf.get('file_name')
    region = kwargs['dag_run'].conf.get('region')
    
    dataset_exists = False
    ti.xcom_push(key='dataset_exists', value=dataset_exists)
    return dataset_exists

#  determine the data source and method to use for fetching
def determine_data_source(**kwargs):
    """
    Determine the data source and fetch method based on dataset name
    """
    ti = kwargs['ti']
    dataset_name = kwargs['dag_run'].conf.get('dataset_name').lower()
    
    source_map = {
        'landsat': {'source': 'aws_s3', 'method': 'fetch_from_aws_open_data'},
        'noaa': {'source': 'api', 'method': 'fetch_from_noaa_api'},
        'mtbs': {'source': 'usgs', 'method': 'fetch_from_usgs'},
        'nifc': {'source': 'html_parsing', 'method': 'fetch_from_nifc_api'}
    }
    
    source_info = source_map.get(dataset_name, {'source': 'unknown', 'method': 'unknown'})
    ti.xcom_push(key='source_info', value=source_info)
    return source_info

# different data sources

def fetch_from_aws_open_data(**kwargs):
    """
    Fetch Landsat data from AWS Open Data using S3 API
    """
    ti = kwargs['ti']
    file_name = kwargs['dag_run'].conf.get('file_name')
    
    local_file_path = f"/tmp/{file_name}"
    ti.xcom_push(key='local_file_path', value=local_file_path)
    return local_file_path

def fetch_from_noaa_api(**kwargs):
    """
    Fetch NOAA data using their APIs
    """
    ti = kwargs['ti']
    dataset_name = kwargs['dag_run'].conf.get('dataset_name')
    file_name = kwargs['dag_run'].conf.get('file_name')

    local_file_path = f"/tmp/{file_name}"
    ti.xcom_push(key='local_file_path', value=local_file_path)
    return local_file_path

def fetch_from_usgs(**kwargs):
    """
    Fetch MTBS data from USGS
    """
    ti = kwargs['ti']
    file_name = kwargs['dag_run'].conf.get('file_name')

    local_file_path = f"/tmp/{file_name}"
    ti.xcom_push(key='local_file_path', value=local_file_path)
    return local_file_path

def fetch_from_nifc_api(**kwargs):
    """
    Fetch NIFC data using HTML parsing or available APIs
    """
    ti = kwargs['ti']
    file_name = kwargs['dag_run'].conf.get('file_name')
    
    local_file_path = f"/tmp/{file_name}"
    ti.xcom_push(key='local_file_path', value=local_file_path)
    return local_file_path

def validate_dataset(**kwargs):
    """
    Validate the dataset schema and integrity
    """
    ti = kwargs['ti']
    local_file_path = ti.xcom_pull(key='local_file_path')
    dataset_name = kwargs['dag_run'].conf.get('dataset_name').lower()

    validation_rules = {
        'landsat': ['bbox', 'file_name', 'source'],
        'noaa': ['dataset_name', 'file_name', 'region', 'version'],
        'mtbs': ['file_name', 'bbox', 'version'],
        'nifc': ['file_name', 'url', 'region_code', 'version']
    }
    
    is_valid = True
    
    file_hash = hashlib.sha256(open(local_file_path, 'rb').read()).hexdigest()
    
    ti.xcom_push(key='is_valid', value=is_valid)
    ti.xcom_push(key='file_hash', value=file_hash)
    return is_valid


def check_for_duplicates(**kwargs):
    """
    Check if the dataset is a duplicate based on file hash or name
    """
    ti = kwargs['ti']
    file_hash = ti.xcom_pull(key='file_hash')
    dataset_name = kwargs['dag_run'].conf.get('dataset_name').lower()
    file_name = kwargs['dag_run'].conf.get('file_name')
    
    duplicate_found = False
    ti.xcom_push(key='duplicate_found', value=duplicate_found)
    return duplicate_found

# Function to upload the dataset to S3
def upload_to_s3(**kwargs):
    """
    Upload the dataset to central S3 bucket
    """
    ti = kwargs['ti']
    local_file_path = ti.xcom_pull(key='local_file_path')
    bucket_name = 'central-dataset-bucket'
    dataset_name = kwargs['dag_run'].conf.get('dataset_name').lower()
    file_name = kwargs['dag_run'].conf.get('file_name')
    
    s3_path = f"s3://{bucket_name}/{dataset_name}/{file_name}"
    ti.xcom_push(key='s3_path', value=s3_path)
    return s3_path

def update_metadata(**kwargs):
    """
    Update metadata table with new dataset information
    """
    ti = kwargs['ti']
    s3_path = ti.xcom_pull(key='s3_path')
    file_hash = ti.xcom_pull(key='file_hash')
    dataset_name = kwargs['dag_run'].conf.get('dataset_name').lower()
    file_name = kwargs['dag_run'].conf.get('file_name')
    user_id = ti.xcom_pull(key='user_id')
    
    # extract additional metadata based on dataset type
    metadata = {
        'file_name': file_name,
        'file_hash': file_hash,
        's3_path': s3_path,
        'upload_time': datetime.now().isoformat(),
        'uploaded_by': user_id,
        'source': dataset_name,
    }
    
    if dataset_name == 'landsat' or dataset_name == 'mtbs':
        metadata['bbox'] = kwargs['dag_run'].conf.get('bbox', 'unknown')
    
    if dataset_name == 'noaa':
        metadata['region'] = kwargs['dag_run'].conf.get('region', 'unknown')
    
    if dataset_name == 'nifc':
        metadata['url'] = kwargs['dag_run'].conf.get('url', 'unknown')
        metadata['region_code'] = kwargs['dag_run'].conf.get('region_code', 'unknown')

    logging.info(f"Metadata updated: {metadata}")
    ti.xcom_push(key='metadata', value=metadata)
    return metadata

def log_event(**kwargs):
    """
    Log event details to event_log table
    """
    ti = kwargs['ti']
    dataset_name = kwargs['dag_run'].conf.get('dataset_name').lower()
    file_name = kwargs['dag_run'].conf.get('file_name')
    user_id = ti.xcom_pull(key='user_id')
    
    # Determine event status
    duplicate_found = ti.xcom_pull(key='duplicate_found', default=False)
    dataset_exists = ti.xcom_pull(key='dataset_exists', default=False)
    
    if duplicate_found:
        status = 'duplicate'
    elif dataset_exists:
        status = 'exists'
    else:
        status = 'downloaded'
    
    event_data = {
        'user_id': user_id,
        'dataset_name': dataset_name,
        'file_name': file_name,
        'timestamp': datetime.now().isoformat(),
        'status': status,
        'event_type': 'user_triggered', 
    }

    logging.info(f"Event logged: {event_data}")
    return event_data

# Function to prepare response for user
def prepare_response(**kwargs):
    """
    Prepare HTTP response to send back to user
    """
    ti = kwargs['ti']
    duplicate_found = ti.xcom_pull(key='duplicate_found', default=False)
    dataset_exists = ti.xcom_pull(key='dataset_exists', default=False)
    s3_path = ti.xcom_pull(key='s3_path', default=None)
    
    if duplicate_found:
        response = {
            'status': 'success',
            'message': 'Dataset already exists and was not re-fetched',
            'http_status': 200,
            'path': s3_path
        }
    elif dataset_exists:
        response = {
            'status': 'success',
            'message': 'Dataset found in existing storage',
            'http_status': 200,
            'path': s3_path
        }
    elif s3_path:
        response = {
            'status': 'success',
            'message': 'Dataset successfully fetched and stored',
            'http_status': 200,
            'path': s3_path
        }
    else:
        response = {
            'status': 'error',
            'message': 'Dataset could not be retrieved',
            'http_status': 404,
            'path': None
        }
    
    ti.xcom_push(key='user_response', value=response)
    return response

# Define the tasks
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

authenticate_task = PythonOperator(
    task_id='authenticate_user',
    python_callable=authenticate_user,
    provide_context=True,
    dag=dag,
)

check_existence_task = PythonOperator(
    task_id='check_dataset_existence',
    python_callable=check_dataset_existence,
    provide_context=True,
    dag=dag,
)

determine_source_task = PythonOperator(
    task_id='determine_data_source',
    python_callable=determine_data_source,
    provide_context=True,
    dag=dag,
)

# Create dynamic tasks based on data source
landsat_task = PythonOperator(
    task_id='fetch_landsat_data',
    python_callable=fetch_from_aws_open_data,
    provide_context=True,
    dag=dag,
)

noaa_task = PythonOperator(
    task_id='fetch_noaa_data',
    python_callable=fetch_from_noaa_api,
    provide_context=True,
    dag=dag,
)

mtbs_task = PythonOperator(
    task_id='fetch_mtbs_data',
    python_callable=fetch_from_usgs,
    provide_context=True,
    dag=dag,
)

nifc_task = PythonOperator(
    task_id='fetch_nifc_data',
    python_callable=fetch_from_nifc_api,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_dataset',
    python_callable=validate_dataset,
    provide_context=True,
    dag=dag,
)

check_duplicates_task = PythonOperator(
    task_id='check_for_duplicates',
    python_callable=check_for_duplicates,
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

update_metadata_task = PythonOperator(
    task_id='update_metadata',
    python_callable=update_metadata,
    provide_context=True,
    dag=dag,
)

log_event_task = PythonOperator(
    task_id='log_event',
    python_callable=log_event,
    provide_context=True,
    dag=dag,
)

prepare_response_task = PythonOperator(
    task_id='prepare_response',
    python_callable=prepare_response,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

# Define the task dependencies
start_task >> authenticate_task >> check_existence_task

# If dataset exists, skip to response
check_existence_task >> prepare_response_task

# If dataset doesn't exist, determine source and fetch
check_existence_task >> determine_source_task

# Branch based on data source
determine_source_task >> [landsat_task, noaa_task, mtbs_task, nifc_task]

# All data fetching tasks converge to validation
[landsat_task, noaa_task, mtbs_task, nifc_task] >> validate_task

# Continue with processing pipeline
validate_task >> check_duplicates_task >> upload_task >> update_metadata_task >> log_event_task >> prepare_response_task

# All paths converge to end task
prepare_response_task >> end_task