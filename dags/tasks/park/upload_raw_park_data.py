import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_CONN_ID = 'aws_default'
S3_KEY_TEMPLATE_PARK = "raw/park/glac_park_info.json"
S3_KEY_TEMPLATE_THINGSTODO = 'raw/park/glac_thingstodo.json'
BUCKET = 'glacier-national-park'
PARK_INFO_TEMP_FILE = '/tmp/glac_park_info.json'
THINGSTODO_TEMP_FILE = '/tmp/glac_thingstodo.json'

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

        s3_hook.load_file(
            filename=THINGSTODO_TEMP_FILE,
            key=S3_KEY_TEMPLATE_THINGSTODO,
            bucket_name=BUCKET,
            replace=True
        )
        print(f'File successfully uploaded to S3 at: s3://{BUCKET}/{S3_KEY_TEMPLATE_THINGSTODO}')
    except Exception as e:
        print(f'Error during S3 upload: {e}')
        raise
    finally:
        if os.path.exists(PARK_INFO_TEMP_FILE):
            os.remove(PARK_INFO_TEMP_FILE)
            print(f'Temporary file deleted: {PARK_INFO_TEMP_FILE}')
        if os.path.exists(THINGSTODO_TEMP_FILE):
            os.remove(THINGSTODO_TEMP_FILE)
            print(f'Temporary file deleted: {THINGSTODO_TEMP_FILE}')