import pandas as pd
import numpy
import json
import re
from datetime import datetime
from utils.constants import raw_path, processed_path

#####################################
## Observation Tranformation function
#####################################

def observation_data(json_data:dict, display:str):
    
    observation = json_data.get("entry",[])
    records =[]
    
    for obs in observation:
        resource = obs["resource"]
        try:
            id = resource["id"]
            patient_id = resource["subject"]["reference"].split("/")[-1]
            if not patient_id.isdigit(): raise Exception()
            code = resource["code"]["coding"][0]["code"] 

            
            if code != '85354-9':
                value = resource["valueQuantity"]["value"]
                unit = resource["valueQuantity"]["unit"]
            
            # In case of Systolic Blood
            elif code == '85354-9' and display=="Systolic Blood":
                code = resource["component"][1]["code"]["coding"][0]["code"]
                value = resource["component"][1]["valueQuantity"]["value"]
                unit = resource["component"][1]["valueQuantity"]["unit"]
            #In case of Diastolic Blood
            else :
                code = resource["component"][0]["code"]["coding"][0]["code"]
                value = resource["component"][0]["valueQuantity"]["value"]
                unit = resource["component"][0]["valueQuantity"]["unit"]
            
            timestamp = resource["effectiveDateTime"]
            
            # Append valid record to the list
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
    # Convert to data frame
    df_observation = pd.DataFrame(records)
    df_observation["timestamp"]=pd.to_datetime(df_observation["timestamp"],errors="coerce",format="mixed")
    df_observation["timestamp"] = df_observation["timestamp"].apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notnull(x) else None)
    df_observation.dropna(inplace=True)
    return df_observation


###################################
## Patients Tranformation function
###################################
def patients_data(json_data):
    entries = json_data.get("entry", [])
    patient_data = []

    for entry in entries:
        try:
            resource = entry.get("resource", {})

            patient_id = resource["id"]

            # Most of the names are mixing with numbers so we have to clean them first
            full_name = " ".join(resource["name"][0]["given"])
            cleaned_name = re.sub(r"\d+","",full_name)

            gender = resource["gender"]
            birth_date = resource["birthDate"]

            patient_data.append({
                "patient_id": patient_id,
                "name": cleaned_name,
                "gender": gender,
                "birth_date": birth_date,
            })

        except Exception as e:
            print(f"Skipping invalid entry due to error: {e}")
            continue

    df_patient = pd.DataFrame(patient_data)
    df_patient["birth_date"]=pd.to_datetime(df_patient["birth_date"],errors="coerce").dt.strftime("%Y-%m-%d")
    df_patient.dropna(inplace=True)
    return df_patient


####################################
## Conditions Tranformation function
####################################
def conditions_data(json_data):
    entries = json_data.get("entry", [])
    condition_data = []

    for entry in entries:
        try:
            resource = entry.get("resource", {})
            condition_id = resource["id"]
            patient_id = resource["subject"]["reference"].split("/")[-1]
            code = resource["code"]["coding"][0]["code"]
            display = resource["code"]["coding"][0]["display"]
            onset_date = resource["onsetDateTime"]

            condition_data.append({
                "id": condition_id,
                "patient_id": patient_id,
                "code": code,
                "display": display,
                "onset_date": onset_date,
            })

        except Exception as e:
            print(f"Skipping invalid entry due to error: {e}")
            continue

    df_condition = pd.DataFrame(condition_data)
    df_condition["onset_date"]=pd.to_datetime(df_condition["onset_date"],errors="coerce",format="mixed")
    df_condition["onset_date"] = df_condition["onset_date"].apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notnull(x) else None)
    df_condition.dropna(inplace=True)
    return df_condition


