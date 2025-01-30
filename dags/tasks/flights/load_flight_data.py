import os
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_flight"
SNOWFLAKE_TABLE = 'FLIGHT_DATA'

RUN_DATE = datetime.now().strftime('%Y-%m-%d')
FLIGHT_DATE = (datetime.now() + timedelta(weeks=1)).strftime('%Y-%m-%d')

def load_data_to_snowflake(**kwargs):
    try:
        ti = kwargs['ti']
        temp_file = ti.xcom_pull(task_ids='transform_data')

        if temp_file is None:
            raise ValueError('No file returned from transform_data task')

        df = pd.read_csv(temp_file)
        
        flight_info = df.iloc[0]

        legs = json.loads(flight_info['legs'])
        logging.info(f'Legs: {legs}')
        carriers = json.loads(flight_info['carriers'])

        sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} (token, total_price, num_legs, departure, arrival, legs, carriers, flight_time, flight_date, run_date)
        SELECT
            '{flight_info['token']}',
            {flight_info['total_price']},
            {flight_info['num_legs']},
            '{flight_info['departure']}',
            '{flight_info['arrival']}',
            ARRAY_CONSTRUCT({", ".join([f"'{x}'" for x in legs])}),
            ARRAY_CONSTRUCT({", ".join([f"'{x}'" for x in carriers])}),
            {flight_info['flight_time']},
            '{FLIGHT_DATE}',
            '{RUN_DATE}'
        WHERE NOT EXISTS (
            SELECT 1
            FROM {SNOWFLAKE_TABLE}
            WHERE token = '{flight_info['token']}'
        );
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