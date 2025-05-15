import os
import pandas as pd
from dotenv import load_dotenv
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from utils.constants import processed_path

def load_to_db(**kwargs):
    load_dotenv()

    # Paths for processed files
    patients_csv = os.path.join(processed_path, "patients.csv")
    observations_csv = os.path.join(processed_path, "observations.csv")
    conditions_csv = os.path.join(processed_path, "conditions.csv")

    try:
        # Load all processed CSV files
        patients_df = pd.read_csv(patients_csv)
        observations_df = pd.read_csv(observations_csv)
        conditions_df = pd.read_csv(conditions_csv)

        # Insert statements

        ## Insert patients to DB
        insert_patients_sql = """
            INSERT INTO patients (patient_id, name, gender, birth_date)
            VALUES
        """ + ",".join([
            f"('{row['patient_id']}', '{row['name']}', '{row['gender']}', '{row['birth_date']}')" 
            for _, row in patients_df.iterrows()
        ]) + """
            ON CONFLICT (patient_id) DO NOTHING;
        """
        
        ## Insert observations to DB
        insert_observations_sql = """
            INSERT INTO observations (id, patient_id, code, display, value, unit, timestamp)
            VALUES
        """ + ",".join([
            f"('{row['id']}', '{row['patient_id']}', '{row['code']}', '{row['display']}', {row['value']}, '{row['unit']}', '{row['timestamp']}')" 
            for _, row in observations_df.iterrows()
        ]) + ";"

        ## Insert conditions to DB
        insert_conditions_sql = """
            INSERT INTO conditions (id, patient_id, code, display, onset_date)
            VALUES
        """ + ",".join([
            f"('{row['id']}', '{row['patient_id']}', '{row['code']}', '{row['display']}', '{row['onset_date']}')" 
            for _, row in conditions_df.iterrows()
        ]) + ";"



        # Execute all 3 loads using PostgresOperator
        load_patients = PostgresOperator(
            task_id="load_patients",
            sql=insert_patients_sql,
            postgres_conn_id="healthcare_postgres",
            autocommit=True,
        )

        load_observations = PostgresOperator(
            task_id="load_observations",
            sql=insert_observations_sql,
            postgres_conn_id="healthcare_postgres",
            autocommit=True,
        )

        load_conditions = PostgresOperator(
            task_id="load_conditions",
            sql=insert_conditions_sql,
            postgres_conn_id="healthcare_postgres",
            autocommit=True,
        )

        # Execute them manually inside the function
        load_patients.execute(context=kwargs)
        load_observations.execute(context=kwargs)
        load_conditions.execute(context=kwargs)

        print("All data loaded successfully!")

    except Exception as e:
        print("Loaded columns:", observations_df.columns)
        print("Loaded columns:", patients_df.columns)
        print("Loaded columns:", conditions_df.columns)
        print(f"Error loading data to database: {e}")
