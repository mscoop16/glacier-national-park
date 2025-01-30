from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os

S3_CONN_ID = 'aws_default'
S3_TRANSFORMED_KEY_TEMPLATE = "{{ ds }}/transformed_hotels.csv"
S3_KEY_TEMPLATE = "raw/hotels/{{ ds }}/best_hotels.json"
BUCKET = 'glacier-national-park'

SNOWFLAKE_CONN_ID = "snowflake_flight"
SNOWFLAKE_TABLE = 'HOTEL_DATA'

def load_data_to_snowflake(**kwargs):
    try:
        ti = kwargs['ti']
        s3_path = ti.xcom_pull(task_ids='transform_hotel_data')

        if s3_path is None:
            raise ValueError('No file path returned from transform_hotel_data task')
        
        transformed_s3_key = S3_TRANSFORMED_KEY_TEMPLATE.replace("{{ ds }}", kwargs['ds'])

        sql = f"""
        COPY INTO {SNOWFLAKE_TABLE}
        FROM @my_s3_stage/{transformed_s3_key}
        FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
        """

        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        snowflake_hook.run(sql)

        print(f"Data successfully loaded into Snowflake table: {SNOWFLAKE_TABLE}")
    except Exception as e:
        print(f"Error loading data into Snowflake: {e}")
        raise