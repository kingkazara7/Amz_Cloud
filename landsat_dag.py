from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLDatabaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
import json
from datetime import datetime

from landsat import query_landsat_stac  # Assuming this is your function somewhere

# Define default args if needed
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id="landsat_stac_processing",
    default_args=default_args,
    schedule_interval=None,
    params={
        "datetime": "2015-07-01/2015-08-01",
        "collection": ["landsat-c2l2-sr"],
        "intersects": (-115.359,35.6763,-113.6548,36.4831),  # Assuming it's a geojson dict or similar
    },
    catchup=False,
    tags=["landsat", "gcp"],
) as dag:

    @task()
    def check_metadata_exists(userparams):
        datetime_param = userparams["datetime"]
        collection_param = userparams["collection"]
        
        start_date, end_date = datetime_param.split("/")
        
        query_sql = """
            SELECT 1 FROM landsat 
            WHERE START_DAY = %s AND END_DAY = %s AND COLLECTION = %s
            LIMIT 1
        """
        hook = MySqlHook(mysql_conn_id='mysql')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query_sql, (start_date, end_date, collection_param))
        rows = cursor.fetchall()
        if rows:
            update_sql = """
                UPDATE landsat
                SET UPDATED = %s
                WHERE START_DAY = %s AND END_DAY = %s AND COLLECTION = %s
            """
            cursor.execute(update_sql, (datetime.today().strftime('%Y-%m-%d'), start_date, end_date, collection_param))
            cursor.close()
            conn.commit()
            conn.close()
            return True
        else:
            insert_sql = """
                INSERT INTO landsat (START_DAY, END_DAY, COLLECTION, UPDATED)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (start_date, end_date, collection_param, datetime.today().strftime('%Y-%m-%d')))
            cursor.close()
            conn.commit()
            conn.close()
            return False

    @task()
    def query_stac_if_needed(metadata_exists, userparams):
        if metadata_exists:
            return None
        
        intersects = userparams["intersects"]
        datetime_param = userparams["datetime"]
        collection_param = userparams["collection"]
        
        stac_dict = query_landsat_stac(intersects=intersects, datetime=datetime_param, collections=collection_param)
        return stac_dict

    @task()
    def upload_to_gcs(stac_result, userparams):
        if not stac_result:
            return
        datetime_param = userparams["datetime"]
        collection_param = userparams["collection"]

        start_date, end_date = datetime_param.split("/")
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        bucket_name = "landsat-de"
        object_name = f"landsat_stac_{collection_param}_{start_date}_{end_date}.json"

        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=object_name,
            data=json.dumps(stac_result),
            mime_type="application/json"
        )

    # DAG execution flow
    metadata_exists = check_metadata_exists(dag.params)
    stac_result = query_stac_if_needed(metadata_exists, dag.params)
    upload_result = upload_to_gcs(stac_result, dag.params)

    done = EmptyOperator(task_id="done")

    upload_result >> done

