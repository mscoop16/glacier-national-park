import pandas as pd
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_CONN_ID = 'aws_default'
S3_KEY_TEMPLATE = "raw/hotels/{{ ds }}/best_hotels.json"
BUCKET = 'glacier-national-park'

def transform_hotel_data(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_key = S3_KEY_TEMPLATE.replace("{{ ds }}", kwargs['ds'])
        raw_data = s3_hook.read_key(key=s3_key, bucket_name=BUCKET)
        data = json.loads(raw_data)

        hotels = data.get('data', {})

        df = pd.DataFrame()
        temp_file = '/tmp/transformed_hotel_data.csv'
        df.to_csv(temp_file, index=False)

        return temp_file

    except Exception as e:
        print(f'Error during transformation: {e}')
        raise