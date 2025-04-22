import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from etls.extract import extract_observation, extract_patients, extract_conditions
from etls.transform import transform_observation, transform_patients, transform_conditions
from etls.load import load_to_db

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG("healthcare_etl_pipeline", default_args=default_args, schedule_interval="@once") as dag:

    extract_observation = PythonOperator(
        task_id="extract_observation",
        python_callable=extract_observation,
    )

    extract_patients = PythonOperator(
        task_id="extract_patients",
        python_callable=extract_patients,
    )

    extract_conditions = PythonOperator(
        task_id="extract_conditions",
        python_callable=extract_conditions,
    )

    transform_observation = PythonOperator(
        task_id="transform_observation",
        python_callable=transform_observation,
    )

    transform_patients = PythonOperator(
        task_id="transform_patients",
        python_callable=transform_patients,
    )

    transform_conditions = PythonOperator(
        task_id="transform_conditions",
        python_callable=transform_conditions,
    )

    load_task = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_db, #connect to ID
        provide_context=True, # Provide the context to PostgresOperator
    )

    # Task dependencies
    extract_patients >> transform_patients
    extract_observation >> transform_observation
    extract_conditions >> transform_conditions

    [transform_patients, transform_observation, transform_conditions] >> load_task


