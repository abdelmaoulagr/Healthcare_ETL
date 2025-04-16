# Healthcare ETL Pipeline with Apache Airflow

This project implements an Extract, Transform, Load (ETL) pipeline to process healthcare data from a **FHIR API**, transform it into a structured format, and load it into a PostgreSQL database for analysis. The pipeline is orchestrated using **Apache Airflow** and integrates with Tableau for visualization.


## 🚀 Features


This ETL pipeline processes **patient heart rate vitals** from a public healthcare API and loads them into a PostgreSQL database. The pipeline is orchestrated with **Apache Airflow** and runs on a **Dockerized environment**.

- **Extract:** Fetches heart rate observations from the FHIR API (`http://hapi.fhir.org/baseR4/Observation?code=8867-4`) and saves them as JSON in `data/raw/`.
- **Transform:** Processes the JSON data into a structured CSV (`data/processed/transformed_data.csv`) with patient ID, heart rate, and timestamp.
- **Load:** Imports the CSV data into a PostgreSQL table (`patient_vitals` in `healthcare_db`).
- **Orchestration:** Managed by an Airflow DAG (`healthcare_etl_pipeline`) running daily.

## 📦 Technologies Used

- [Apache Airflow](https://airflow.apache.org/)
- [Docker](https://www.docker.com/)
- [PostgreSQL](https://www.postgresql.org/)
- Python (pandas, psycopg2, sqlalchemy)

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/abdelmaoulagr/Healthcare_ETL.git
cd Healthcare_ETL
```

### 2. Create a Virtual Environment 

```bash
python3 -m venv venv
source venv/bin/activate
```
### 3. Install the Dependencies
```bash
pip install -r requirements.txt
```
### 4. Start Docker Containers

Make sure Docker and Docker Compose are installed.

```bash
docker-compose up --build
```
### 5. Access Airflow
- Airflow Webserver: http://localhost:8080

    - Default credentials: `airflow / airflow`

### 6. Trigger the DAG
- Go to the Airflow UI

- Turn on and trigger the `healthcare_etl_pipeline` DAG

