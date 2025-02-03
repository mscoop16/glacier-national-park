from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os

S3_CONN_ID = os.environ.get('S3_CONN_ID')
S3_KEY_TEMPLATE = "raw/park/glac_park_info.json"
S3_KEY_TEMPLATE_TRANSFORMED = 'park/park_images.csv'
BUCKET = os.environ.get('BUCKET')

SNOWFLAKE_CONN_ID = os.environ.get('SNOWFLAKE_CONN_ID')
SNOWFLAKE_TABLE = 'PARK_IMAGES'

def load_data_to_snowflake(**kwargs):
    try:
        ti = kwargs['ti']
        s3_path = ti.xcom_pull(task_ids='transform_park_data')

        if s3_path is None:
            raise ValueError('No file path returned from transform_hotel_data task')
        

        sql = f"""
        COPY INTO {SNOWFLAKE_TABLE}
        FROM @my_s3_stage/{S3_KEY_TEMPLATE_TRANSFORMED}
        FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
        """

        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        snowflake_hook.run(sql)

        print(f"Data successfully loaded into Snowflake table: {SNOWFLAKE_TABLE}")
    except Exception as e:
        print(f"Error loading data into Snowflake: {e}")
        raise