import polars as pl
import json
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_CONN_ID = 'aws_default'
S3_KEY_TEMPLATE = "raw/park/glac_park_info.json"
S3_KEY_TEMPLATE_TRANSFORMED = 'transformed/park/park_images.csv'
BUCKET = 'glacier-national-park'

def transform_park_data(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        # Park Info
        raw_data = s3_hook.read_key(key=S3_KEY_TEMPLATE, bucket_name=BUCKET)
        data = json.loads(raw_data)

        park_info = data.get('data', {})[0]
        images = park_info['images']

        park_images = []
        for image in images:
            park_images.append((image['title'], image['url']))
        df = pl.DataFrame(
            park_images,
            schema=['title', 'url'],
            orient='row'
        )

        csv_buffer = StringIO()
        df.write_csv(csv_buffer)
        csv_buffer.seek(0)

        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=S3_KEY_TEMPLATE_TRANSFORMED,
            bucket_name=BUCKET,
            replace=True
        )

        return f"s3://{BUCKET}/{S3_KEY_TEMPLATE_TRANSFORMED}"

    except Exception as e:
        print(f'Error during transformation: {e}')
        raise