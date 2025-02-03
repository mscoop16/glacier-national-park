import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_CONN_ID = os.environ.get('S3_CONN_ID')
S3_KEY_TEMPLATE_PARK = "raw/park/glac_park_info.json"
BUCKET = os.environ.get('BUCKET')
PARK_INFO_TEMP_FILE = '/tmp/glac_park_info.json'

def upload_parkdata_to_S3(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.load_file(
            filename=PARK_INFO_TEMP_FILE,
            key=S3_KEY_TEMPLATE_PARK,
            bucket_name=BUCKET,
            replace=True
        )
        print(f'File successfully uploaded to S3 at: s3://{BUCKET}/{S3_KEY_TEMPLATE_PARK}')
    except Exception as e:
        print(f'Error during S3 upload: {e}')
        raise
    finally:
        if os.path.exists(PARK_INFO_TEMP_FILE):
            os.remove(PARK_INFO_TEMP_FILE)
            print(f'Temporary file deleted: {PARK_INFO_TEMP_FILE}')