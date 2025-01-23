from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import requests
import json
import os

S3_CONN_ID = 'aws_default'
S3_KEY_TEMPLATE = "raw/flights/{{ ds }}/phl_to_mso.json"
BUCKET = 'glacier-national-park'
TEMP_FILE = '/tmp/phl_to_mso.json'

API_URL = "https://booking-com15.p.rapidapi.com/api/v1/flights/searchFlights"
API_KEY = Variable.get('booking-api-key')
API_QUERY = {"fromId":"PHL.AIRPORT",
                   "toId":"MSO.AIRPORT",
                   "departDate":"2025-01-24",
                   "pageNo":"1",
                   "adults":"1",
                   "sort":"BEST",
                   "cabinClass":"ECONOMY",
                   "currency_code":"USD"}
API_HEADERS = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "booking-com15.p.rapidapi.com"
    }


name = 'mscoop'

def fetch_flight_data(**kwargs):
    try:
        response = requests.get(API_URL, headers=API_HEADERS, params=API_QUERY)
        response.raise_for_status()
        data = response.json()
        
        with open(TEMP_FILE, 'w') as f:
            json.dump(data, f)
        print(f'Data successfully written to temp file {TEMP_FILE}')
    except Exception as e:
        print(f'Error during API call: {e}')
        raise

def upload_to_S3(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_key = S3_KEY_TEMPLATE.replace("{{ ds }}", kwargs['ds'])
        s3_hook.load_file(
            filename=TEMP_FILE,
            key=s3_key,
            bucket_name=BUCKET,
            replace=True
        )

        print(f'File successfully uploaded to S3 at: s3://{BUCKET}/{s3_key}')
    except Exception as e:
        print(f'Error during S3 upload: {e}')
        raise
    finally:
        if os.path.exists(TEMP_FILE):
            os.remove(TEMP_FILE)
            print(f'Temporary file deleted: {TEMP_FILE}')

default_args = {
    'owner': 'mscoop',
    'retries' : 0
}
with DAG(
    'flight_flow',
    default_args=default_args,
    start_date=datetime(2025, 1, 22),
    catchup=False
) as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_flight_data',
        python_callable=fetch_flight_data,
        provide_context=True
    )

    upload_data_task = PythonOperator(
        task_id='upload_to_S3',
        python_callable=upload_to_S3,
        provide_context=True
    )

    fetch_data_task >> upload_data_task