from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from tasks.flights.get_flight_data import fetch_flight_data
from tasks.flights.upload_raw_flight_data import upload_flight_to_S3
from tasks.flights.transform_flight_data import transform_flight_data
from tasks.flights.load_flight_data import load_flight_data_to_snowflake

from tasks.hotel.get_hotel_data import fetch_hotel_data
from tasks.hotel.upload_raw_hotel_data import upload_hotel_to_S3
from tasks.hotel.transform_hotel_data import transform_hotel_data
from tasks.hotel.load_hotel_data import load_hotel_data_to_snowflake

from datetime import datetime, timedelta
import os

SNOWFLAKE_CONN_ID = Variable.get('SNOWFLAKE_CONN_ID_GLAC')
SNOWFLAKE_TABLE_FLIGHT = 'FLIGHT_DATA'
SNOWFLAKE_TABLE_HOTEL = 'HOTEL_DATA'

name = 'mscoop'
default_args = {
    'owner': 'mscoop',
    'retries' : 0
}
with DAG(
    'glacier_flow',
    schedule_interval=None,
    description = 'A DAG that performs ETL on hotel & flight data for a trip to Glacier National Park',
    default_args=default_args,
    start_date=datetime(2025, 1, 31),
    catchup=False,
    template_searchpath='/usr/local/airflow/include/sql/'
) as dag:
    fetch_flight_data_task = PythonOperator(
        task_id='fetch_flight_data',
        python_callable=fetch_flight_data,
        provide_context=True
    )

    upload_flight_data_task = PythonOperator(
        task_id='upload_flight_to_S3',
        python_callable=upload_flight_to_S3,
        provide_context=True
    )

    transform_flight_data_task = PythonOperator(
        task_id='transform_flight_data',
        python_callable=transform_flight_data,
        provide_context=True
    )

    upload_transformed_flight_data_task = PythonOperator(
        task_id='upload_flight_to_snowflake',
        python_callable=load_flight_data_to_snowflake,
        provide_context=True
    )

    fetch_hotel_data_task = PythonOperator(
        task_id='fetch_hotel_data',
        python_callable=fetch_hotel_data,
        provide_context=True
    )

    upload_hotel_data_task = PythonOperator(
        task_id='upload_hotel_data_to_S3',
        python_callable=upload_hotel_to_S3,
        provide_context=True
    )

    transform_hotel_data_task = PythonOperator(
        task_id='transform_hotel_data',
        python_callable=transform_hotel_data,
        provide_context=True
    )

    upload_transformed_hotel_data_task = PythonOperator(
        task_id='upload_hotel_data_to_snowflake',
        python_callable=load_hotel_data_to_snowflake,
        provide_context=True
    )

    with TaskGroup(group_id='quality_check_group_flight') as quality_check_group_flight:
        flight_column_checks = SQLColumnCheckOperator(
            conn_id=SNOWFLAKE_CONN_ID,
            task_id ='flight_column_checks',
            table=SNOWFLAKE_TABLE_FLIGHT,
            column_mapping={"token": {"null_check": {"equal_to": 0}}}
        )
        flight_row_check = SnowflakeCheckOperator(
            task_id='flight_row_checks',
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql='row_quality_flight_table.sql',
            params={'table': SNOWFLAKE_TABLE_FLIGHT}
        )
    
    with TaskGroup(group_id='quality_check_group_hotel') as quality_check_group_hotel:
        hotel_row_check = SnowflakeCheckOperator(
            task_id='hotel_row_checks',
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql='row_quality_hotel_table.sql',
            params={'table': SNOWFLAKE_TABLE_HOTEL}
        )
        
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")
        
    chain(
        begin,
        [fetch_flight_data_task, fetch_hotel_data_task],
        [upload_flight_data_task, upload_hotel_data_task],
        [transform_flight_data_task, transform_hotel_data_task],
        [upload_transformed_flight_data_task, upload_transformed_hotel_data_task],
        [quality_check_group_flight, quality_check_group_hotel],
        end
    )