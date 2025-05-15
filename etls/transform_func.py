import pandas as pd
import numpy
import json
import re
from datetime import datetime
from utils.constants import raw_path, processed_path

#####################################
## Observation Tranformation Function
#####################################

def observation_data(json_data:dict, display:str):
    """
    Transforms raw FHIR Observation JSON into a structured DataFrame.

    Parameters:
    - json_data (dict): Raw JSON data from the FHIR API.
    - display (str): A human-readable label (e.g., "Heart Rate", "Systolic Blood", "Diastolic Blood").

    Returns:
    - pd.DataFrame: A cleaned and structured DataFrame containing:
        - id: Observation ID
        - patient_id: Patient reference ID
        - code: LOINC code for the observation
        - display: Provided display label
        - value: Observation numeric value
        - unit: Unit of measurement
        - timestamp: Time the observation was recorded (ISO format)
    """


    observation = json_data.get("entry",[])
    records =[]
    
    for obs in observation:
        resource = obs["resource"]
        try:
            id = resource["id"]
            patient_id = resource["subject"]["reference"].split("/")[-1]
            if not patient_id.isdigit(): raise Exception() # Skip non-numeric patient IDs
            code = resource["code"]["coding"][0]["code"] 

            # Handle standard single-value observations like Heart Rate, Temperature.
            if code != '85354-9': # 85354-9 is a composite code for Blood Pressure
                value = resource["valueQuantity"]["value"]
                unit = resource["valueQuantity"]["unit"]

            # Handle Blood Pressure (code: 85354-9) – which includes two components:
            elif code == '85354-9' and display=="Systolic Blood":
                # Systolic is the second component (index 1)
                code = resource["component"][1]["code"]["coding"][0]["code"]
                value = resource["component"][1]["valueQuantity"]["value"]
                unit = resource["component"][1]["valueQuantity"]["unit"]
            else :
                # Diastolic is the first component (index 0)
                code = resource["component"][0]["code"]["coding"][0]["code"]
                value = resource["component"][0]["valueQuantity"]["value"]
                unit = resource["component"][0]["valueQuantity"]["unit"]
            
            timestamp = resource["effectiveDateTime"]
            
            # Append valid observation record
            records.append(
                {
                "id":id,
                "patient_id": patient_id,
                "code":code,
                "display":display,
                "value": value,
                "unit": unit,
                "timestamp": timestamp
                })
    
        except:
            continue
    # Convert the list of records into a DataFrame
    df_observation = pd.DataFrame(records)
    # Convert timestamp to standardized ISO format
    df_observation["timestamp"]=pd.to_datetime(
        df_observation["timestamp"],
        errors="coerce",
        format="mixed"
    )
    df_observation["timestamp"] = df_observation["timestamp"].apply(
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notnull(x) else None
    )
    # Drop rows with any missing values
    df_observation.dropna(inplace=True)

    return df_observation


###################################
## Patients Tranformation Function
###################################
def patients_data(json_data):
    """
    Transforms raw FHIR Patient JSON data into a structured DataFrame.

    Parameters:
    - json_data (dict): Raw JSON data from the FHIR API containing patient records.

    Returns:
    - pd.DataFrame: A cleaned DataFrame with the following columns:
        - patient_id: Unique identifier for the patient
        - name: Cleaned full name of the patient
        - gender: Gender of the patient
        - birth_date: Birth date in YYYY-MM-DD format
    """

    entries = json_data.get("entry", [])
    patient_data = []

    for entry in entries:
        try:
            resource = entry.get("resource", {})

            # Extract patient ID
            patient_id = resource["id"]

            # Most of the names are mixing with numbers so we have to clean them first
            # Extract and clean the patient's full name
            # Some names contain numbers (e.g., from test data), so remove digits
            full_name = " ".join(resource["name"][0]["given"])
            cleaned_name = re.sub(r"\d+","",full_name)

            # Extract gender and birth date
            gender = resource["gender"]
            birth_date = resource["birthDate"]

            # Append the cleaned and structured patient data
            patient_data.append({
                "patient_id": patient_id,
                "name": cleaned_name,
                "gender": gender,
                "birth_date": birth_date,
            })

        except Exception as e:
            # Skip any malformed entries and print the error for debugging
            print(f"Skipping invalid entry due to error: {e}")
            continue
    
    # Convert the list of patient records into a DataFrame
    df_patient = pd.DataFrame(patient_data)

    # Format birth_date to standard YYYY-MM-DD format
    df_patient["birth_date"]=pd.to_datetime(
        df_patient["birth_date"],
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    
    # Drop rows with missing or invalid data
    df_patient.dropna(inplace=True)

    return df_patient


####################################
## Conditions Tranformation Function
####################################
def conditions_data(json_data):
    """
    Transforms raw FHIR Condition JSON data into a structured DataFrame.

    Parameters:
    - json_data (dict): Raw JSON data from the FHIR API containing condition records.

    Returns:
    - pd.DataFrame: A cleaned DataFrame with the following columns:
        - id: Unique identifier for the condition
        - patient_id: Reference ID of the patient with the condition
        - code: Standardized condition code (e.g., ICD or SNOMED)
        - display: Human-readable name of the condition
        - onset_date: The date/time the condition started (formatted)
    """


    entries = json_data.get("entry", [])
    condition_data = []

    for entry in entries:
        try:
            resource = entry.get("resource", {})
            # Extract condition metadata
            condition_id = resource["id"]
            # Extract patient ID from the 'subject' reference
            patient_id = resource["subject"]["reference"].split("/")[-1]

            # Extract medical code and human-readable name for the condition
            code = resource["code"]["coding"][0]["code"]
            display = resource["code"]["coding"][0]["display"]
            # Extract onset date of the condition
            onset_date = resource["onsetDateTime"]

            # Append the record to the list
            condition_data.append({
                "id": condition_id,
                "patient_id": patient_id,
                "code": code,
                "display": display,
                "onset_date": onset_date,
            })

        except Exception as e:
            # Log and skip any malformed or incomplete entries
            print(f"Skipping invalid entry due to error: {e}")
            continue
    
    # Convert condition's data into a DataFrame
    df_condition = pd.DataFrame(condition_data)

    # Convert onset_date to a unified timestamp format
    df_condition["onset_date"]=pd.to_datetime(
        df_condition["onset_date"],
        errors="coerce",
        format="mixed"
    )
    df_condition["onset_date"] = df_condition["onset_date"].apply(
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notnull(x) else None
    )
    
    # Drop rows with missing or invalid onset_date
    df_condition.dropna(inplace=True)

    return df_condition


