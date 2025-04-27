import os
import tempfile
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago

from fetch_helper import build_noaa_url, fetch_and_store_from_api

# Environment / bucket settings
AWS_BUCKET = os.getenv("CENTRAL_BUCKET", "central-dataset-bucket")
REPORT_PREFIX = "reports/noaa"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="noaa_dataset_processing_and_reporting",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # daily at 06:00 UTC
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
    tags=["noaa", "report"],
) as dag:

    @task()
    def check_noaa_metadata(userparams):
        """
        Returns True if a metadata record exists for today's request, else False.
        """
        hook = MySqlHook(mysql_conn_id="mysql")
        fn = f"{userparams['datasetid']}_{userparams['startdate']}_{userparams['enddate']}_{userparams['stationid'].replace(':','_')}.json"
        sql = """
            SELECT 1
              FROM noaa_metadata
             WHERE file_name = %s
             LIMIT 1
        """
        return hook.get_first(sql, parameters=(fn,)) is not None

    @task()
    def fetch_noaa_data_if_needed(metadata_exists, userparams):
        """
        Fetches from NOAA API + uploads to S3 if metadata does not already exist.
        Returns the S3 URI or None.
        """
        if metadata_exists:
            return None
        url = build_noaa_url(
            datasetid=userparams["datasetid"],
            startdate=userparams["startdate"],
            enddate=userparams["enddate"],
            stationid=userparams["stationid"],
        )
        return fetch_and_store_from_api(source_url=url, dataset_name="noaa")

    @task()
    def update_noaa_metadata(s3_uri, userparams):
        """
        Inserts a row into noaa_metadata for a newly fetched file.
        """
        if not s3_uri:
            return
        hook = MySqlHook(mysql_conn_id="mysql")
        fn = f"{userparams['datasetid']}_{userparams['startdate']}_{userparams['enddate']}_{userparams['stationid'].replace(':','_')}.json"
        hook.run(
            """
            INSERT INTO noaa_metadata
              (dataset_name, file_name, region, version, request_user, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                userparams["datasetid"],
                fn,
                userparams["region"],
                userparams["version"],
                userparams["request_user"],
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    @task()
    def generate_noaa_report(userparams):
        """
        Generates a CSV report of the last 7 days of fetch metadata and uploads to S3.
        Returns the S3 URI of the report.
        """
        hook = MySqlHook(mysql_conn_id="mysql")
        df = hook.get_pandas_df(
            """
            SELECT *
              FROM noaa_metadata
             WHERE dataset_name = %s
               AND timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            """,
            parameters=(userparams["datasetid"],),
        )
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        report_fn = f"noaa_report_{ts}.csv"
        df.to_csv(tmp.name, index=False)
        tmp.close()

        s3 = S3Hook(aws_conn_id="aws_default")
        key = f"{REPORT_PREFIX}/{report_fn}"
        s3.load_file(
            filename=tmp.name,
            key=key,
            bucket_name=AWS_BUCKET,
            replace=True,
        )
        os.remove(tmp.name)
        return f"s3://{AWS_BUCKET}/{key}"

    # Orchestration
    checked = check_noaa_metadata(dag.params)
    fetched = fetch_noaa_data_if_needed(checked, dag.params)
    updated = update_noaa_metadata(fetched, dag.params)
    report = generate_noaa_report(dag.params)
    done = EmptyOperator(task_id="done")

    checked >> fetched >> updated >> report >> done
