import requests
import json
import os
from utils.constants import raw_path



def extract_fhir_data(url: str,filename: str):
    file_path=f"{raw_path}/{filename}.json"

    # Delete the file if it already exists
    if os.path.exists(file_path):
        os.remove(file_path)

    
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "w") as f:
            json.dump(response.json(), f)
        print(f"{filename} extracted successfully!")
    else:
        raise Exception(f"API request failed with status code {response.status_code}")




# API Endpoints
## Observations 
heart_rate_url='http://host.docker.internal:8081/fhir/Observation?code=8867-4'
blood_pressure_url='http://host.docker.internal:8081/fhir/Observation?code=85354-9'
body_tempreture_url='http://host.docker.internal:8081/fhir/Observation?code=8310-5'

## Patients
patient_url='http://host.docker.internal:8081/fhir/Patient'

## Conditions
condition_url='http://host.docker.internal:8081/fhir/Condition'


# Extract the observation
def extract_observation():

    # Heart rate data extraction
    extract_fhir_data(heart_rate_url,"heart_rate")

    # Blood pressure extraction
    extract_fhir_data(blood_pressure_url,'blood_pressure')

    # Body tempreture extraction
    extract_fhir_data(body_tempreture_url,'body_tempreture')

# Extract Patients
def extract_patients():
    extract_fhir_data(patient_url,'patients')


# Extract Conditions
def extract_conditions():
    extract_fhir_data(condition_url,"conditions")