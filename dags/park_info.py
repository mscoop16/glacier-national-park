from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator

from tasks.park.get_park_data import fetch_park_data
from tasks.park.upload_raw_park_data import upload_parkdata_to_S3
from tasks.park.transform_park_data import transform_park_data
from tasks.park.load_park_data import load_data_to_snowflake

from datetime import datetime

default_args = {
    'owner': 'mscoop',
    'retries' : 0
}
with DAG(
    'park_info_flow',
    schedule_interval=None,
    description = 'A DAG that performs ETL on park information on Glacier National Park',
    default_args=default_args,
    start_date=datetime(2025, 1, 31),
    catchup=False
) as dag:
    fetch_park_data_task = PythonOperator(
        task_id='fetch_park_data',
        python_callable=fetch_park_data,
        provide_context=True
    )

    upload_park_data_task = PythonOperator(
        task_id='upload_park_data_to_S3',
        python_callable=upload_parkdata_to_S3,
        provide_context=True
    )

    transform_park_data_task = PythonOperator(
        task_id='transform_park_data',
        python_callable=transform_park_data,
        provide_context=True
    )

    upload_transformed_park_data_task = PythonOperator(
        task_id='upload_park_data_to_snowflake',
        python_callable=load_data_to_snowflake,
        provide_context=True
    )

    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")
        
    chain(
        begin,
        fetch_park_data_task,
        upload_park_data_task,
        transform_park_data_task,
        upload_transformed_park_data_task,
        end
    )