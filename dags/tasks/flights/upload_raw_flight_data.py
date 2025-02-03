import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_CONN_ID = os.environ.get('S3_CONN_ID')
S3_KEY_TEMPLATE = "raw/flights/{{ ds }}/phl_to_mso.json"
BUCKET = os.environ.get('BUCKET')
TEMP_FILE = '/tmp/phl_to_mso.json'

def upload_flight_to_S3(**kwargs):
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