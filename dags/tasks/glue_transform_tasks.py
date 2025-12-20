"""
Glue Transform Job tasks for OpenAQ data pipeline.
Handles PySpark transformation of raw measurements via AWS Glue job.
"""
import sys
sys.path.insert(0, '/opt/airflow/')

from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from pipelines.glue_pipeline import trigger_glue_transform_job, check_glue_transform_status
from utils.constants import GLUE_TRANSFORM_JOB_NAME


def create_trigger_glue_transform_task(dag, job_name: str = None, retries: int = 1):
    """
    Create a task to trigger Glue PySpark transformation job.

    This job:
    - Reads raw JSON measurements from S3 (aq_raw/)
    - Applies transformations (datetime parsing, deduplication, pivot)
    - Enriches with location metadata
    - Writes partitioned Parquet to S3 (aq_dev/marts/)

    Args:
        dag: Airflow DAG object
        job_name: Name of Glue job (uses config default if None)
        retries: Number of retries on failure

    Returns:
        PythonOperator: Trigger Glue job task
    """
    task = PythonOperator(
        task_id='trigger_glue_transform_job',
        python_callable=trigger_glue_transform_job,
        op_kwargs={'job_name': job_name or GLUE_TRANSFORM_JOB_NAME},
        provide_context=True,
        dag=dag,
        retries=retries,
        retry_delay=timedelta(minutes=2),
        doc="""
        Trigger AWS Glue PySpark transformation job.

        Input: Raw JSON measurements from OpenAQ extraction
        - Location: s3://bucket/aq_raw/year/month/day/hour/*.json

        Transformations:
        - Parse datetime strings to timestamps
        - Deduplicate by location_id + datetime
        - Pivot parameter columns (PM2.5, PM10, NO2, etc.)
        - Enrich with location metadata (coordinates, city, country)
        - Extract year/month/day partition columns

        Output: Partitioned Parquet files
        - Location: s3://bucket/aq_dev/marts/year=YYYY/month=MM/day=DD/*.parquet
        - Partitioned by: location_id, year, month, day
        """
    )

    return task


def create_wait_glue_transform_task(
    dag,
    poke_interval: int = 60,
    timeout: int = 7200,
    mode: str = 'poke'
):
    """
    Create a sensor task to wait for Glue job completion.

    Polls job status and waits for completion (success or failure).

    Args:
        dag: Airflow DAG object
        poke_interval: Check job status every N seconds (default: 60 = 1 minute)
        timeout: Timeout in seconds (default: 7200 = 2 hours)
        mode: 'poke' for simple polling or 'reschedule' for slot-free waiting

    Returns:
        PythonSensor: Wait for Glue job task
    """
    task = PythonSensor(
        task_id='wait_glue_transform_job',
        python_callable=check_glue_transform_status,
        poke_interval=poke_interval,
        timeout=timeout,
        mode=mode,
        dag=dag,
        doc=f"""
        Poll Glue job until completion.

        - Checks job status every {poke_interval} seconds
        - Timeout: {timeout / 60:.0f} minutes
        - Mode: {mode}
        - Raises exception if job fails
        """
    )

    return task


def create_glue_transform_tasks(dag):
    """
    Create all Glue transform tasks (trigger + wait).

    Creates a complete pipeline stage:
    1. trigger_glue_transform_job: Start the Glue PySpark job
    2. wait_glue_transform_job: Wait for job completion

    Args:
        dag: Airflow DAG object

    Returns:
        tuple: (trigger_task, wait_task)
    """
    trigger_task = create_trigger_glue_transform_task(dag)
    wait_task = create_wait_glue_transform_task(dag)

    return trigger_task, wait_task
