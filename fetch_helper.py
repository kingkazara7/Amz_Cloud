import os
import hashlib
import tempfile
import requests
import boto3
import botocore
from datetime import datetime
import urllib.parse as ul

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
CENTRAL_BUCKET = os.getenv("CENTRAL_BUCKET", "central-dataset-bucket")
NOAA_TOKEN = os.getenv("NOAA_API_TOKEN")
s3 = boto3.client("s3", region_name=AWS_REGION)

def build_noaa_url(datasetid, startdate, enddate=None, stationid=None, limit=1000):
    base = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    params = {
        "datasetid": datasetid,
        "startdate": startdate,
        "limit": limit
    }
    if enddate:
        params["enddate"] = enddate
    if stationid:
        params["stationid"] = stationid
    return f"{base}?{ul.urlencode(params)}"

def fetch_and_store_from_api(source_url, dataset_name, file_name=None):
    if not file_name:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        hsh = hashlib.sha1(source_url.encode()).hexdigest()[:10]
        file_name = f"{ts}_{hsh}.json"

    key = f"{dataset_name}/{file_name}"
    s3_uri = f"s3://{CENTRAL_BUCKET}/{key}"

    try:
        s3.head_object(Bucket=CENTRAL_BUCKET, Key=key)
        print("[dedup] File already exists:", s3_uri)
        return s3_uri
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise

    print("[download] Fetching data from:", source_url)
    headers = {"token": NOAA_TOKEN} if NOAA_TOKEN else {}
    response = requests.get(source_url, headers=headers, timeout=90)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(response.content)
        tmp_path = tmp.name

    file_hash = hashlib.sha256(response.content).hexdigest()
    print("[validate] SHA-256:", file_hash)

    print("[upload] Uploading to:", s3_uri)
    s3.upload_file(tmp_path, CENTRAL_BUCKET, key)
    os.remove(tmp_path)

    return s3_uri
