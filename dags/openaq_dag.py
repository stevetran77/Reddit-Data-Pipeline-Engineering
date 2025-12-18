import sys
sys.path.insert(0, '/opt/airflow/')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from pipelines.openaq_pipeline import openaq_pipeline

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Generate daily filename with date
file_postfix = datetime.now().strftime("%Y%m%d")

# Define DAG
dag = DAG(
    dag_id='openaq_to_s3_pipeline',
    default_args=default_args,
    description='Daily air quality data extraction to AWS S3',
    schedule_interval='@daily',  # Runs at midnight daily
    catchup=False,
    tags=['openaq', 'airquality', 'etl', 's3', 'pipeline']
)

# Task: Extract air quality data for Hanoi
extract_hanoi = PythonOperator(
    task_id='extract_hanoi_airquality',
    python_callable=openaq_pipeline,
    op_kwargs={
        'file_name': f'hanoi_{file_postfix}',
        'city': 'Hanoi',
        'country': 'VN',
        'lookback_hours': 24
    },
    dag=dag
)

# Example: Extract for multiple cities (optional)
extract_hcmc = PythonOperator(
    task_id='extract_hcmc_airquality',
    python_callable=openaq_pipeline,
    op_kwargs={
        'file_name': f'hcmc_{file_postfix}',
        'city': 'Ho Chi Minh City',
        'country': 'VN',
        'lookback_hours': 24
    },
    dag=dag
)

# Tasks run in parallel (no dependencies)
# To run sequentially: extract_hanoi >> extract_hcmc
