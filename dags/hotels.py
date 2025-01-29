from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator

from tasks.hotel.get_hotel_data import fetch_hotel_data
from tasks.hotel.upload_raw_hotel_data import upload_to_S3
from tasks.hotel.transform_hotel_data import transform_hotel_data
from tasks.hotel.load_hotel_data import load_data_to_snowflake

from datetime import datetime

default_args = {
    'owner': 'mscoop',
    'retries' : 0
}
with DAG(
    'hotel_flow',
    schedule_interval=None,
    description = 'A DAG that performs ETL on hotel data for a trip to Glacier National Park',
    default_args=default_args,
    start_date=datetime(2025, 1, 29),
    catchup=False
) as dag:
    fetch_hotel_data_task = PythonOperator(
        task_id='fetch_hotel_data',
        python_callable=fetch_hotel_data,
        provide_context=True
    )

    upload_hotel_data_task = PythonOperator(
        task_id='upload_hotel_data_to_S3',
        python_callable=upload_to_S3,
        provide_context=True
    )

    transform_hotel_data_task = PythonOperator(
        task_id='transform_hotel_data',
        python_callable=transform_hotel_data,
        provide_context=True
    )

    upload_transformed_hotel_data_task = PythonOperator(
        task_id='upload_hotel_data_to_snowflake',
        python_callable=load_data_to_snowflake,
        provide_context=True
    )

    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")
        
    chain(
        begin,
        fetch_hotel_data_task,
        upload_hotel_data_task,
        transform_hotel_data_task,
        upload_transformed_hotel_data_task,
        end
    )