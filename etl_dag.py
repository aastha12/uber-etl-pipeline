from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from etl import run_uber_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 25),
    'email': ['aasthajha123@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'uber_dag',
    default_args=default_args,
    description='Uber ETL DAG',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='uber_etl',
    python_callable=run_uber_etl,
    dag=dag, 
)

run_etl