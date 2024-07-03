from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os

from api import get_and_transform_artists_data, insert_data

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
    description = 'Importa informacion sobre el top 10 tracks de algunos artistas',
     schedule_interval = timedelta(minutes = 1),
    catchup = False
)

artists = ['79R7PUc6T6j09G8mJzNml2', '4EmjPNMuvvKSEAyx7ibGrs', '3HrbmsYpKjWH1lzhad7alj']

def get_and_transform_data_callable(**kwargs):
    artists = kwargs['artists']
    data = get_and_transform_artists_data(artists)
    return data

# def create_connection_callable():
#     conn = create_redshift_connection()
#     return conn

def insert_data_callable(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_and_transform_data')
   # conn = ti.xcom_pull(task_ids='create_connection')
    insert_data(data)


task_1 = PythonOperator(
    task_id = 'get_and_transform_data',
    python_callable=get_and_transform_data_callable,
    op_kwargs={'artists': artists},

    dag = etl_pipeline_dag,
)

# task_2 = PythonOperator(
#     task_id='create_connection',
#     python_callable=create_connection_callable,
#     dag = etl_pipeline_dag,
# )

task_3 = PythonOperator(
    task_id = 'insert_data',
    python_callable=insert_data_callable,
    provide_context=True,
    dag = etl_pipeline_dag,
)



task_1 >> task_3