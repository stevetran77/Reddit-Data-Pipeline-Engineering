"""
Extract tasks for OpenAQ data pipeline.
Handles extraction of air quality data for different cities.
"""
import sys
sys.path.insert(0, '/opt/airflow/')

from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pipelines.openaq_pipeline import openaq_pipeline

def create_extract_task(dag, city: str, country: str, lookback_hours: int = 24, retries: int = 3, max_retries: int = 3):
    """
    Create an extraction task for a specific city with improved error handling.

    Args:
        dag: Airflow DAG object
        city: City name (e.g., "Hanoi")
        country: Country code (e.g., "VN")
        lookback_hours: Hours to look back in time
        retries: Number of Airflow retries on failure
        max_retries: Max API request retries per sensor

    Returns:
        PythonOperator: Extraction task
    """
    file_postfix = datetime.now().strftime("%Y%m%d_%H%M")
    city_lower = city.lower().replace(' ', '_')

    task = PythonOperator(
        task_id=f'extract_{city_lower}_airquality',
        python_callable=openaq_pipeline,
        op_kwargs={
            'file_name': f'{city_lower}_{file_postfix}',
            'city': city,
            'country': country,
            'lookback_hours': lookback_hours,
            'api_limit': 1000,  # Fix: OpenAQ API max limit
            'max_sensor_retries': max_retries,
            'parameters': ['pm25', 'pm10']  # Focus on key pollutants
        },
        dag=dag,
        retries=retries,
        retry_delay=timedelta(minutes=5),  # Wait 5 min between retries
        on_failure_callback=lambda context: print(f"Failed extract task: {context['task_instance'].task_id}")
    )

    return task

def create_extraction_tasks(dag, cities_config: list):
    """
    Create extraction tasks for multiple cities with parallel execution support.

    Args:
        dag: Airflow DAG object
        cities_config: List of dicts with city config
                       [{'city': 'Hanoi', 'country': 'VN', 'lookback_hours': 24}, ...]

    Returns:
        list: List of extraction task operators
    """
    extraction_tasks = []

    for city_config in cities_config:
        city = city_config.get('city')
        country = city_config.get('country')
        lookback_hours = city_config.get('lookback_hours', 24)
        retries = city_config.get('retries', 3)
        max_sensor_retries = city_config.get('max_sensor_retries', 3)

        task = create_extract_task(
            dag, 
            city, 
            country, 
            lookback_hours, 
            retries, 
            max_sensor_retries
        )
        extraction_tasks.append(task)

    return extraction_tasks
