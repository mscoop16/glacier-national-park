from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
import requests
import json
import os

S3_CONN_ID = 'aws_default'
S3_KEY_TEMPLATE = "raw/flights/{{ ds }}/phl_to_mso.json"
BUCKET = 'glacier-national-park'
TEMP_FILE = '/tmp/phl_to_mso.json'

SNOWFLAKE_TABLE = 'FLIGHT_DATA'

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

def transform_data(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_key = S3_KEY_TEMPLATE.replace("{{ ds }}", kwargs['ds'])
        raw_data = s3_hook.read_key(key=s3_key, bucket_name=BUCKET)
        data = json.loads(raw_data)

        flights = data.get('data', {})

        if 'error' in flights:
            print(f'Error returned by API: {flights['error']['code']}')
            return

        flight_deals = flights.get('flightDeals', [])
        best_flight_deal = next((flight for flight in flight_deals if flight['key'] == 'BEST'), None)
        if not best_flight_deal:
            raise ValueError("No 'best' flight deal found.")
        token = best_flight_deal['offerToken']

        flight_offers = flights.get('flightOffers', [])
        best_flight = next((flight for flight in flight_offers if flight['token'] == token), None)
        if not best_flight:
            raise ValueError("No matching 'best' flight offer found.")
        
        segment = best_flight['segments'][0]

        departure = segment['departureAirport']['code']
        arrival = segment['arrivalAirport']['code']
        total_price = best_flight['priceBreakdown']['total']['units'] # In USD per request

        legs = segment['legs']
        ordered_airports = []
        carriers = set()
        for leg in legs:
            if not ordered_airports or ordered_airports[-1] != leg['departureAirport']['code']:
                ordered_airports.append(leg['departureAirport']['code'])
            ordered_airports.append(leg['arrivalAirport']['code'])
            carriers.update(leg['carriers'])

        num_legs = len(legs)
        departure_dt = parser.isoparse(segment['departureTime'])
        arrival_dt = parser.isoparse(segment['arrivalTime'])
        flight_time = round((arrival_dt - departure_dt).total_seconds() / 3600, 2)

        flight_info = {
            'token': token,
            'total_price': total_price,
            'num_legs': num_legs,
            'departure': departure,
            'arrival': arrival,
            'legs': ordered_airports,
            'carriers': list(carriers),
            'flight_time': flight_time
        }

        df = pd.DataFrame([flight_info])
        return df

    except Exception as e:
        print(f'Error during transformation: {e}')
        raise

def load_data_to_snowflake(**kwargs):
    try:
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='transform_data')

        if df is None:
            raise ValueError('No data returned from transform_data task')
        
        flight_info = df[0]

        sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} (token, total_price, num_legs, departure, arrival, legs, carriers, flight_time)
        VALUES (
            '{flight_info['token']}',
            {flight_info['total_price']},
            {flight_info['num_legs']},
            '{flight_info['departure']}',
            '{flight_info['arrival']}',
            ARRAY_CONSTRUCT({", ".join([f"'{x}'" for x in flight_info['legs']])}),
            ARRAY_CONSTRUCT({", ".join([f"'{x}'" for x in flight_info['carriers']])}),
            {flight_info['flight_time']}
        );
        """

        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        snowflake_hook.run(sql)

        print(f"Data successfully loaded into Snowflake table: {SNOWFLAKE_TABLE}")
    except Exception as e:
        print(f"Error loading data into Snowflake: {e}")
        raise

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

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    upload_transformed_data_task = PythonOperator(
        task_id='upload_to_snowflake',
        python_callable=load_data_to_snowflake,
        provide_context=True
    )

    fetch_data_task >> upload_data_task >> transform_data_task >> upload_transformed_data_task