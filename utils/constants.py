import configparser
import os
from pathlib import Path

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))


# File Paths
raw_path = Path("data/raw/")
processed_path = Path("data/processed/")

# API Endpoints
fhir_api='http://host.docker.internal:8081/fhir'
## Observations 
heart_rate_url=f'{fhir_api}/Observation?code=8867-4'
blood_pressure_url=f'{fhir_api}/Observation?code=85354-9'
body_tempreture_url=f'{fhir_api}/Observation?code=8310-5'

## Patients
patient_url=f'{fhir_api}/Patient'

## Conditions
condition_url=f'{fhir_api}/Condition'