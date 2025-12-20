import sys
sys.path.insert(0, '/opt/airflow/')

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from tasks.catalog_tasks import create_catalog_tasks
from tasks.validation_tasks import create_validate_athena_task
from tasks.glue_transform_tasks import create_glue_transform_tasks
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
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# Define DAG
dag = DAG(
    dag_id='openaq_to_athena_pipeline',
    default_args=default_args,
    description='Extract air quality data from OpenAQ, load to S3, catalog with Glue Crawler, validate with Athena',
    schedule_interval=None,  # Runs at midnight daily
    catchup=False,
    tags=['openaq', 'airquality', 'etl', 's3', 'glue', 'athena', 'pipeline']
)

# NEW: Create Vietnam-wide extraction task (extract all locations)
# Replaces city-based extraction (Hanoi, HCMC) for efficiency
extract_all_vietnam = PythonOperator(
    task_id='extract_all_vietnam_locations',
    python_callable=openaq_pipeline,
    op_kwargs={
        'file_name': 'vietnam_national_{{ ts_nodash }}',
        'vietnam_wide': True,  # Enable Vietnam-wide extraction
        'lookback_hours': 24,
        'api_limit': 1000,
        'max_sensor_retries': 3,
        'parameters': ['pm25', 'pm10', 'no2', 'so2', 'o3', 'co']
    },
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=5)
)

# Single extraction task (contains all Vietnam locations including Hanoi and HCMC)
extraction_tasks = [extract_all_vietnam]

# Create Glue transform tasks (trigger + wait)
# These transform raw JSON to partitioned Parquet using Spark
trigger_glue_transform_task, wait_glue_transform_task = create_glue_transform_tasks(dag)

# Create catalog tasks (trigger + wait)
# Glue Crawler scans S3 marts and updates Glue Data Catalog
trigger_crawler_task, wait_crawler_task = create_catalog_tasks(dag)

# Create validation task
# Verify data is queryable in Athena
validate_task = create_validate_athena_task(dag)

# Task Dependencies (Updated Pipeline Flow)
# 1. Extract raw measurements from OpenAQ API
# 2. Transform raw JSON to Parquet using Spark (Glue job)
# 3. Catalog transformed data using Glue Crawler
# 4. Validate data in Athena
extract_all_vietnam >> trigger_glue_transform_task >> wait_glue_transform_task >> trigger_crawler_task >> wait_crawler_task >> validate_task
