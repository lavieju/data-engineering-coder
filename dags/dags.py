from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os

from api import get_and_transform_artists_data, insert_data
from utils import send_email

dag_path = os.getcwd()
default_args = {
    'owner': 'julian',
    'start_date': datetime(2024, 7, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes = 5)
}

etl_pipeline_dag = DAG(
    dag_id = 'ingestion_data',
    default_args = default_args,
    description = 'Import information about top 10 tracks of given artists',
    schedule_interval = timedelta(minutes = 3),
    catchup = False
)

artists = ['79R7PUc6T6j09G8mJzNml2', '4EmjPNMuvvKSEAyx7ibGrs', '3HrbmsYpKjWH1lzhad7alj']

def get_and_transform_data_callable(**kwargs):
    artists = kwargs['artists']
    data = get_and_transform_artists_data(artists)
    return data

def insert_data_callable(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_and_transform_data')
    result = insert_data(data)
    return result

def send_email_callable(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='insert_data')
    send_email(result)


task_1 = PythonOperator(
    task_id = 'get_and_transform_data',
    python_callable=get_and_transform_data_callable,
    op_kwargs={'artists': artists},

    dag = etl_pipeline_dag,
)

task_2 = PythonOperator(
    task_id = 'insert_data',
    python_callable=insert_data_callable,
    provide_context=True,
    dag = etl_pipeline_dag,
)

task_3 =  PythonOperator(
    task_id='send_mail',
    python_callable=send_email_callable,
    provide_context=True,
    dag=etl_pipeline_dag,
)


task_1 >> task_2 >> task_3