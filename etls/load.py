import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
from utils.constants import processed_path

def load_to_db():
    load_dotenv()
    POSTGRES_USER=os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD=os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST=os.getenv('POSTGRES_HOST')
    POSTGRES_PORT=os.getenv('POSTGRES_PORT')
    POSTGRES_DB=os.getenv('POSTGRES_DB')
    # Database connection
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()
        print("Connected to the database successfully!")
        
        # Load CSV
        df = pd.read_csv(processed_path)
        
        # Insert data into the table
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO patient_vitals (patient_id, heart_rate, timestamp) VALUES (%s, %s, %s)
                """,
                (row['patient_id'], row['heart_rate'], row['timestamp'])
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Data loaded to database successfully!")
    
    except Exception as e:
        print(f"Error: {e}")
