import polars as pl
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_CONN_ID = 'aws_default'
S3_KEY_TEMPLATE_PARK = "raw/park/glac_park_info.json"
S3_KEY_TEMPLATE_THINGSTODO = 'raw/park/glac_thingstodo.json'
BUCKET = 'glacier-national-park'

def transform_park_data(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        # Park Info
        raw_data = s3_hook.read_key(key=S3_KEY_TEMPLATE_PARK, bucket_name=BUCKET)
        data = json.loads(raw_data)

        park_info = data.get('data', {})
        ## Do stuff with park info

    except Exception as e:
        print(f'Error during transformation: {e}')
        raise