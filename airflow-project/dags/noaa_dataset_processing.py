from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime
import json
import os
from fetch_helper import build_noaa_url, fetch_and_store_from_api

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id="noaa_dataset_processing",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "datasetid": "GHCND",
        "startdate": "2024-01-01",
        "enddate": "2024-01-02",
        "stationid": "GHCND:USW00094728",
        "region": "Midwest",
        "version": "v1",
        "request_user": "anonymous_user",
    },
    tags=["noaa", "aws"],
) as dag:

    @task()
    def check_noaa_metadata(userparams):
        file_name = f"{userparams['datasetid']}_{userparams['startdate']}_{userparams['enddate']}_{userparams['stationid'].replace(':', '_')}.json"
        hook = MySqlHook(mysql_conn_id='mysql')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        check_sql = """
            SELECT 1 FROM noaa_metadata WHERE file_name = %s LIMIT 1
        """
        cursor.execute(check_sql, (file_name,))
        exists = cursor.fetchone() is not None
        
        cursor.close()
        conn.close()
        return exists

    @task()
    def fetch_noaa_data_if_needed(metadata_exists, userparams):
        if metadata_exists:
            return None
        
        url = build_noaa_url(
            datasetid=userparams["datasetid"],
            startdate=userparams["startdate"],
            enddate=userparams["enddate"],
            stationid=userparams["stationid"],
            limit=1000
        )
        s3_uri = fetch_and_store_from_api(source_url=url, dataset_name="noaa")
        return s3_uri

    @task()
    def update_noaa_metadata(s3_uri, userparams):
        if not s3_uri:
            return
        
        file_name = f"{userparams['datasetid']}_{userparams['startdate']}_{userparams['enddate']}_{userparams['stationid'].replace(':', '_')}.json"
        
        hook = MySqlHook(mysql_conn_id='mysql')
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO noaa_metadata (dataset_name, file_name, region, version, request_user, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (
            userparams["datasetid"],
            file_name,
            userparams["region"],
            userparams["version"],
            userparams["request_user"],
            datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        ))

        conn.commit()
        cursor.close()
        conn.close()

    # Define execution flow
    metadata_exists = check_noaa_metadata(dag.params)
    s3_uri = fetch_noaa_data_if_needed(metadata_exists, dag.params)
    upload_metadata = update_noaa_metadata(s3_uri, dag.params)
    
    done = EmptyOperator(task_id="done")
    
    upload_metadata >> done
