import os
# Manually set NOAA token here so fetch_helper picks it up
os.environ['NOAA_API_TOKEN'] = 'fzYHgOYMrfcXkBKpNeTMJJmnYhFQQRzx'

from flask import Flask, request, send_file, jsonify, render_template, redirect, url_for
import requests
import boto3
import tempfile
import time
import json
from datetime import datetime
import logging

# Import NOAA helper functions (will see the token from environment)
from fetch_helper import build_noaa_url, fetch_and_store_from_api

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration (loaded from environment variables or defaults)
AIRFLOW_URL      = os.getenv('AIRFLOW_URL', 'http://52.14.48.134:8080')
S3_BUCKET        = os.getenv('S3_BUCKET', 'central-dataset-bucket')
AIRFLOW_USERNAME = os.getenv('AIRFLOW_USERNAME', 'airflow')
AIRFLOW_PASSWORD = os.getenv('AIRFLOW_PASSWORD', 'airflow')
AWS_REGION       = os.getenv('AWS_REGION', 'us-east-2')

# Ensure boto3 uses correct region
boto3.setup_default_session(region_name=AWS_REGION)

# --- ROUTES -------------------------------------------------

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api')
def api_docs():
    return render_template('api.html')

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/list_dags')
def list_dags():
    try:
        endpoint = f"{AIRFLOW_URL}/api/v1/dags"
        resp = requests.get(endpoint, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
        resp.raise_for_status()
        return jsonify({"status": "success", "dags": resp.json().get('dags', [])})
    except Exception as e:
        logger.exception("Error listing DAGs")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/list_files_ui')
def list_files_ui():
    try:
        prefix = request.args.get('prefix', 'noaa/')
        s3 = boto3.client('s3')
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        files = [{'key': o['Key'], 'size': o['Size'], 'last_modified': o['LastModified'].isoformat()} for o in resp.get('Contents', [])]
        return render_template('files.html', files=files, prefix=prefix)
    except Exception as e:
        logger.exception("Error listing files UI")
        return f"Error: {e}", 500

@app.route('/download_file')
def download_file():
    try:
        key = request.args.get('key')
        if not key:
            return jsonify({"status": "error", "message": "Missing key"}), 400
        s3 = boto3.client('s3')
        fd, tmp_path = tempfile.mkstemp()
        os.close(fd)
        s3.download_file(S3_BUCKET, key, tmp_path)
        mimetype = 'application/json' if key.endswith('.json') else 'text/csv'
        return send_file(tmp_path, as_attachment=True, download_name=os.path.basename(key), mimetype=mimetype)
    except Exception as e:
        logger.exception("Error downloading file")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/trigger_dag')
def trigger_dag():
    try:
        dag_id = request.args.get('dag_id', 'noaa_dataset_processing_and_reporting')
        params = {k: request.args[k] for k in ['datasetid','startdate','enddate','stationid','region','version','request_user'] if k in request.args}
        params.setdefault('datasetid','GHCND')
        params.setdefault('region','Midwest')
        params.setdefault('version','v1')
        params.setdefault('request_user','web_api_user')
        if 'startdate' not in params:
            return jsonify({"status": "error", "message": "Missing startdate"}), 400
        params.setdefault('enddate', params['startdate'])
        run_id = f"manual_{int(time.time())}"
        payload = {'dag_run_id': run_id, 'conf': params}
        endpoint = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
        resp = requests.post(endpoint, json=payload, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD), headers={'Content-Type':'application/json'})
        resp.raise_for_status()
        return jsonify({"status": "success", "dag_id": dag_id, "run_id": run_id})
    except Exception as e:
        logger.exception("Error triggering DAG")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/request_data', methods=['POST'])
def request_data():
    try:
        datasetid = request.form.get('datasetid', 'GHCND')
        startdate = request.form.get('startdate')
        enddate   = request.form.get('enddate') or startdate
        stationid = request.form.get('stationid')
        region    = request.form.get('region', 'Midwest')
        wait_flag = request.form.get('wait') == 'true'
        params = {
            'datasetid': datasetid,
            'startdate': startdate,
            'enddate': enddate,
            'stationid': stationid,
            'region': region,
            'wait': 'true' if wait_flag else 'false'
        }
        qs = '&'.join(f"{k}={v}" for k,v in params.items() if v)
        return redirect(f"/process_and_download?{qs}")
    except Exception as e:
        logger.exception("Error in request_data")
        return f"Error: {e}", 500

@app.route('/process_and_download', methods=['GET'])
def process_and_download():
    try:
        params = {k: request.args.get(k) for k in ['datasetid','startdate','enddate','stationid','region','version','request_user']}
        params.setdefault('datasetid','GHCND')
        params.setdefault('region','Midwest')
        params.setdefault('version','v1')
        params.setdefault('request_user','web_api_user')
        if not params['startdate']:
            return jsonify({"status": "error", "message": "Missing startdate"}), 400
        params.setdefault('enddate', params['startdate'])

        # Trigger DAG
        dag_id = request.args.get('dag_id','noaa_dataset_processing_and_reporting')
        run_id = f"manual_{int(time.time())}"
        endpoint = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
        requests.post(endpoint, json={'dag_run_id': run_id, 'conf': params}, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD), headers={'Content-Type':'application/json'}).raise_for_status()

        # Wait if requested
        if request.args.get('wait','true').lower()=='true':
            status_ep = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
            timeout = int(request.args.get('timeout','300'))
            start = time.time()
            while time.time()-start < timeout:
                state = requests.get(status_ep, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)).json().get('state')
                if state=='success': break
                if state in ('failed','error'):
                    return jsonify({"status":"error","message":f"DAG {state}"}),500
                time.sleep(5)

        # Define logical S3 key
        fn = f"{params['datasetid']}_{params['startdate']}_{params['enddate']}_{params['stationid'].replace(':','_')}.json"
        key = f"noaa/{fn}"
        s3 = boto3.client('s3')

        # Try exact download
        fd, tmp_path = tempfile.mkstemp()
        os.close(fd)
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=key)
            s3.download_file(S3_BUCKET, key, tmp_path)
            return send_file(tmp_path, as_attachment=True, download_name=fn, mimetype='application/json')
        except:
            logger.info(f"{key} not in S3")

        # Attempt NOAA fetch (token now set globally)
        try:
            noaa_url = build_noaa_url(
                datasetid=params['datasetid'],
                startdate=params['startdate'],
                enddate=params['enddate'],
                stationid=params['stationid']
            )
            fetch_and_store_from_api(source_url=noaa_url, dataset_name='noaa', file_name=fn)
            s3.download_file(S3_BUCKET, key, tmp_path)
            return send_file(tmp_path, as_attachment=True, download_name=fn, mimetype='application/json')
        except Exception as e:
            logger.warning(f"NOAA fetch failed: {e}")

        # Final fallback: serve most recent timestamped file
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='noaa/')
        objs = resp.get('Contents', [])
        if objs:
            objs.sort(key=lambda o: o['LastModified'], reverse=True)
            best = objs[0]['Key']
            s3.download_file(S3_BUCKET, best, tmp_path)
            return send_file(tmp_path, as_attachment=True, download_name=os.path.basename(best), mimetype='application/json')

        return jsonify({"status":"error","message":"No NOAA data available for the given parameters"}),404
    except Exception as e:
        logger.exception("Error in process_and_download")
        return jsonify({"status":"error","message":str(e)}),500

if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000,debug=True)