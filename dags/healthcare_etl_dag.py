import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from etls.extract import extract_fhir_data
from etls.transform import transform_fhir_data


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG("healthcare_etl_pipeline", default_args=default_args, schedule_interval="@daily") as dag:
    extract_task = PythonOperator(
        task_id="extract_fhir_data",
        python_callable=extract_fhir_data,
    )
    transform_task = PythonOperator(
        task_id="transform_fhir_data",
        python_callable=transform_fhir_data,
    )
    # load_task = PythonOperator(
    #     task_id="load_to_db",
    #     python_callable=load_to_db,
    # )

    extract_task >> transform_task 


