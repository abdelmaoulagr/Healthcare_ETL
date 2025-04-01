import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from utils.constants import processed_path

def load_to_db(**kwargs):
    load_dotenv()
    POSTGRES_USER=os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD=os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST=os.getenv('POSTGRES_HOST')
    POSTGRES_PORT=os.getenv('POSTGRES_PORT')
    POSTGRES_DB=os.getenv('POSTGRES_DB')
    # Database connection
    try:
        # conn = psycopg2.connect(
        #     dbname=POSTGRES_DB,
        #     user=POSTGRES_USER,
        #     password=POSTGRES_PASSWORD,
        #     host=POSTGRES_HOST,
        #     port=POSTGRES_PORT
        # )
        # cursor = conn.cursor()

        # Load CSV data
        df = pd.read_csv(processed_path)
        
        # print("Connected to the database successfully!")
        # Create SQL insert command for bulk loading data
        insert_sql = """
            INSERT INTO patient_vitals (patient_id, heart_rate, timestamp) 
            VALUES
        """ + ",".join([f"({row['patient_id']}, {row['heart_rate']}, '{row['timestamp']}')" for _, row in df.iterrows()]) + """
            ON CONFLICT (patient_id) DO NOTHING;
        """
        # Define PostgresOperator to execute the insert SQL
        load_task = PostgresOperator(
            task_id="load_patient_vitals",
            sql=insert_sql,
            postgres_conn_id="healthcare_postgres",
            autocommit=True,
        )
          # Execute the task
        load_task.execute(context=kwargs)
        print("Data loaded to database successfully!")
    
    except Exception as e:
        print(f"Error: {e}")
