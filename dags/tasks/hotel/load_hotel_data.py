from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow.models import Variable

S3_CONN_ID = Variable.get('S3_CONN_ID_GLAC')
S3_TRANSFORMED_KEY_TEMPLATE = "hotels/{{ ds }}/transformed_hotels.csv"
S3_KEY_TEMPLATE = "raw/hotels/{{ ds }}/best_hotels.json"
BUCKET = Variable.get('GLACIER_BUCKET')

RUN_DATE = datetime.now().strftime('%Y-%m-%d')

SNOWFLAKE_CONN_ID = Variable.get('SNOWFLAKE_CONN_ID_GLAC')
SNOWFLAKE_TABLE = 'HOTEL_DATA'

def load_hotel_data_to_snowflake(**kwargs):
    try:
        ti = kwargs['ti']
        s3_path = ti.xcom_pull(task_ids='transform_hotel_data')

        if s3_path is None:
            raise ValueError('No file path returned from transform_hotel_data task')
        
        transformed_s3_key = S3_TRANSFORMED_KEY_TEMPLATE.replace("{{ ds }}", kwargs['ds'])

        sql = f"""
            DECLARE data_count INTEGER;

            BEGIN
                LET data_count = (SELECT COUNT(*) FROM {SNOWFLAKE_TABLE} WHERE RUN_DATE = '{RUN_DATE}');

                IF data_count > 0 THEN
                    COPY INTO {SNOWFLAKE_TABLE}
                    FROM @my_s3_stage/{transformed_s3_key}
                    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
                ELSE
                    RETURN 'Data already exists for this date.';
                END IF;
            END;
        """


        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        snowflake_hook.run(sql)

        print(f"Data successfully loaded into Snowflake table: {SNOWFLAKE_TABLE}")
    except Exception as e:
        print(f"Error loading data into Snowflake: {e}")
        raise