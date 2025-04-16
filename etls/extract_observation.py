# import pandas as pd
import json
from etls.extract import extract_fhir_data
from utils.constants import raw_path

# API Endpoints
heart_rate_url='http://hapi.fhir.org/baseR4/Observation?code=8867-4'
systolic_blood_url='http://hapi.fhir.org/baseR4/Observation?code=8480-6'
diastolic_blood_url='http://hapi.fhir.org/baseR4/Observation?code=8462-4'
mean_blood_url='http://hapi.fhir.org/baseR4/Observation?code=8478-0'
body_tempreture_url='http://hapi.fhir.org/baseR4/Observation?code=8310-5'

def extract_observation():
# Extract the observation

    # Heart rate data extraction
    heart_rate = extract_fhir_data(heart_rate_url)
    with open(f"{raw_path}/heart_rate.json", "w") as f:
        json.dump(heart_rate, f)
    
    # Systolic blood pressure extraction
    systolic_blood = extract_fhir_data(systolic_blood_url)
    with open(f"{raw_path}/systolic_blood.json", "w") as f:
        json.dump(systolic_blood, f)

    # Diastolic blood pressure extraction
    diastolic_blood = extract_fhir_data(diastolic_blood_url)
    with open(f"{raw_path}/diastolic_blood.json", "w") as f:
        json.dump(diastolic_blood, f)

    # Body tempreture extraction
    body_tempreture = extract_fhir_data(body_tempreture_url)
    with open(f"{raw_path}/body_tempreture.json", "w") as f:
        json.dump(body_tempreture, f)



