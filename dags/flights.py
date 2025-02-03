from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.utils.task_group import TaskGroup

from tasks.flights.get_flight_data import fetch_flight_data
from tasks.flights.upload_raw_flight_data import upload_flight_to_S3
from tasks.flights.transform_flight_data import transform_flight_data
from tasks.flights.load_flight_data import load_flight_data_to_snowflake

from datetime import datetime, timedelta
import os

SNOWFLAKE_CONN_ID = os.environ.get('SNOWFLAKE_CONN_ID')
SNOWFLAKE_TABLE = 'FLIGHT_DATA'

name = 'mscoop'
default_args = {
    'owner': 'mscoop',
    'retries' : 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'flight_flow',
    schedule_interval=None,
    description = 'A DAG that performs ETL on flight data for a trip to Glacier National Park',
    default_args=default_args,
    start_date=datetime(2025, 1, 22),
    catchup=False
) as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_flight_data',
        python_callable=fetch_flight_data,
        provide_context=True
    )

    upload_data_task = PythonOperator(
        task_id='upload_flight_to_S3',
        python_callable=upload_flight_to_S3,
        provide_context=True
    )

    transform_data_task = PythonOperator(
        task_id='transform_flight_data',
        python_callable=transform_flight_data,
        provide_context=True
    )

    upload_transformed_data_task = PythonOperator(
        task_id='upload_flight_to_snowflake',
        python_callable=load_flight_data_to_snowflake,
        provide_context=True
    )

    with TaskGroup(
        group_id='quality_check_group_flight',
        default_args={
            'conn_id' : SNOWFLAKE_CONN_ID
        }
    ) as quality_check_group_flight:
        flight_column_checks = SQLColumnCheckOperator(
        task_id ='flight_column_checks',
        table=SNOWFLAKE_TABLE,
        column_mapping={"token": {"null_check": {"equal_to": 0}}}
    )
        
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")
        
    chain(
        begin,
        fetch_data_task,
        upload_data_task,
        transform_data_task,
        upload_transformed_data_task,
        quality_check_group_flight,
        end
    )