from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import os

SNOWFLAKE_CONN_ID = "snowflake_flight"
SNOWFLAKE_TABLE = 'HOTEL_DATA'

def load_data_to_snowflake(**kwargs):
    try:
        ti = kwargs['ti']
        temp_file = ti.xcom_pull(task_ids='transform_hotel_data')

        if temp_file is None:
            raise ValueError('No file returned from transform_hotel_data task')

        sql = f"""
        COPY INTO {SNOWFLAKE_TABLE}
        FROM {temp_file}
        FILE FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
        """

        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        snowflake_hook.run(sql)

        print(f"Data successfully loaded into Snowflake table: {SNOWFLAKE_TABLE}")

        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f'Temporary file deleted: {temp_file}')
    except Exception as e:
        print(f"Error loading data into Snowflake: {e}")
        raise