import requests
import json
import os
from utils.constants import raw_path, heart_rate_url, blood_pressure_url, body_tempreture_url, patient_url, condition_url




def extract_fhir_data(url: str,filename: str):
    """
    Extracts data from a FHIR API endpoint and saves it as a JSON file in data/raw directory.

    Parameters:
    - url (str): The FHIR API URL to fetch the data from.
    - filename (str): The name (without extension) to save the extracted data as.

    Raises:
    - Exception: If the API request fails (non-200 status code).
    """

    # Construct the file path in the raw data directory
    file_path=f"{raw_path}/{filename}.json"

    # If a file with the same name already exists, remove it to avoid stale data
    if os.path.exists(file_path):
        os.remove(file_path)

    # Send a GET request to the FHIR API
    response = requests.get(url)

    # If the response is successful, save the JSON content to the specified file
    if response.status_code == 200:
        with open(file_path, "w") as f:
            json.dump(response.json(), f)
        print(f"{filename} extracted successfully!")
    else:
        # Raise an error if the API call fails
        raise Exception(f"API request failed with status code {response.status_code}")




# Extract Observations
def extract_observation():

    """
    Extracts multiple types of patient observation data from the FHIR API and 
    saves them as JSON files in the raw data directory.
    """

    # Extract Heart Rate observations
    extract_fhir_data(heart_rate_url,"heart_rate")

    # Extract Blood Pressure observations (contains both Systolic and Diastolic readings)
    extract_fhir_data(blood_pressure_url,'blood_pressure')

    # Extract Body Temperature observations
    extract_fhir_data(body_tempreture_url,'body_tempreture')

# Extract Patients
def extract_patients():
    """
    Extracts patient demographic data from the FHIR API and saves it as a JSON file.
    """
    extract_fhir_data(patient_url,'patients')


# Extract Conditions
def extract_conditions():
    """
    Extracts patient conditions (diagnoses) from the FHIR API and saves them as a JSON file.
    """
    extract_fhir_data(condition_url,"conditions")