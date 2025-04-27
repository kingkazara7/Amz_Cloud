import os
import hashlib
import tempfile
import time
import requests
import boto3
import botocore
from datetime import datetime
import urllib.parse as ul
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
CENTRAL_BUCKET = os.getenv("CENTRAL_BUCKET", "central-dataset-bucket")
NOAA_TOKEN = os.getenv("NOAA_API_TOKEN")

def get_s3_client():
    """Create and return an S3 client with proper configuration"""
    return boto3.client("s3", region_name=AWS_REGION)

def build_noaa_url(datasetid, startdate, enddate=None, stationid=None, limit=1000):
    """Build a URL for the NOAA API request"""
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

def fetch_and_store_from_api(source_url, dataset_name, file_name=None, max_retries=3, retry_delay=5):
    """
    Fetch data from an API and store it in S3 with improved error handling and retries.
    
    Args:
        source_url (str): The URL to fetch data from
        dataset_name (str): Name of the dataset (used for S3 key)
        file_name (str, optional): Custom filename. If None, one will be generated.
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Base delay in seconds between retries (will use exponential backoff)
        
    Returns:
        str: S3 URI where the data is stored
    """
    # Ensure CENTRAL_BUCKET is set
    if not CENTRAL_BUCKET:
        raise ValueError("CENTRAL_BUCKET environment variable is not set")
    
    # Generate filename if not provided
    if not file_name:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        hsh = hashlib.sha1(source_url.encode()).hexdigest()[:10]
        file_name = f"{ts}_{hsh}.json"
    
    # Set up S3 key and URI
    key = f"{dataset_name}/{file_name}"
    s3_uri = f"s3://{CENTRAL_BUCKET}/{key}"
    
    # Initialize S3 client
    s3 = get_s3_client()
    
    # Check if file already exists in S3
    try:
        s3.head_object(Bucket=CENTRAL_BUCKET, Key=key)
        print("[dedup] File already exists:", s3_uri)
        return s3_uri
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404":
            print(f"[error] Unexpected S3 error: {str(e)}")
            raise
    
    # Configure retry strategy for requests
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=retry_delay,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    print("[download] Fetching data from:", source_url)
    
    # Set up headers with token if available
    headers = {}
    if NOAA_TOKEN:
        headers["token"] = NOAA_TOKEN
        # Mask token in logs for security
        masked_token = '*' * (len(NOAA_TOKEN) - 4) + NOAA_TOKEN[-4:] if NOAA_TOKEN else 'None'
        print(f"[debug] Using token: {masked_token}")
    else:
        print("[warning] NOAA_API_TOKEN environment variable not set. Requests may be rate limited.")
    
    # Try to fetch with retries for 503 errors
    try:
        response = session.get(source_url, headers=headers, timeout=90)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 503:
            print(f"[error] Service unavailable (503). This could indicate maintenance or overloaded servers.")
            
            # Manual retry logic with exponential backoff
            for attempt in range(max_retries):
                wait_time = retry_delay * (2**attempt)
                print(f"[retry] Attempt {attempt+1} of {max_retries} after {wait_time} seconds")
                time.sleep(wait_time)
                
                try:
                    response = session.get(source_url, headers=headers, timeout=90)
                    response.raise_for_status()
                    print(f"[retry-success] Successful on attempt {attempt+1}")
                    break
                except requests.exceptions.HTTPError as retry_error:
                    if retry_error.response.status_code != 503 or attempt == max_retries - 1:
                        # If not a 503 error or we've exhausted retries, re-raise
                        print(f"[retry-fail] Failed on attempt {attempt+1}: {str(retry_error)}")
                        if attempt == max_retries - 1:
                            print("[retry-exhausted] All retry attempts failed")
                        raise
        else:
            # Not a 503 error, so re-raise
            raise
    
    # Write content to temp file
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(response.content)
        tmp_path = tmp.name
    
    # Calculate file hash for data integrity
    file_hash = hashlib.sha256(response.content).hexdigest()
    print("[validate] SHA-256:", file_hash)
    
    # Upload to S3
    print("[upload] Uploading to:", s3_uri)
    try:
        s3.upload_file(tmp_path, CENTRAL_BUCKET, key)
        # Add metadata to the object
        s3.put_object_tagging(
            Bucket=CENTRAL_BUCKET,
            Key=key,
            Tagging={
                'TagSet': [
                    {'Key': 'source_url', 'Value': source_url},
                    {'Key': 'hash', 'Value': file_hash},
                    {'Key': 'timestamp', 'Value': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")},
                ]
            }
        )
    except Exception as e:
        print(f"[error] Failed to upload to S3: {str(e)}")
        raise
    finally:
        # Clean up temp file
        os.remove(tmp_path)
    
    return s3_uri