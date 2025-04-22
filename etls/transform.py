import pandas as pd
import numpy
import json
import re
from datetime import datetime
from utils.constants import raw_path, processed_path
from etls.transform_func import observation_data, patients_data, conditions_data


 # Transform Observation Data
def transform_observation():
    # Transform Heart Rate Data

    with open(f'{raw_path}/heart_rate.json',"r") as f:
        heart_rate_data = json.load(f)
    heart_rate = observation_data(heart_rate_data, "Heart Rate")


    # Transform Body Tempreture Data
    with open(f'{raw_path}/body_tempreture.json',"r") as f:
        body_tempreture_data = json.load(f)
    body_tempreture = observation_data(body_tempreture_data, "Body Tempreture")

    # Transform Systolic Blood Data
    with open(f'{raw_path}/blood_pressure.json',"r") as f:
        systolic_blood_data = json.load(f)
    systolic_blood = observation_data(systolic_blood_data, "Systolic Blood")

    # Transform Diastolic Blood Data
    with open(f'{raw_path}/blood_pressure.json',"r") as f:
        diastolic_blood_data = json.load(f)
    diastolic_blood = observation_data(diastolic_blood_data, "Diastolic Blood")

    # Merge the four data frames into one
    df_observations = pd.concat(
        [heart_rate, body_tempreture, systolic_blood, diastolic_blood ],
        ignore_index=True
    )
    df_observations.to_csv(f'{processed_path}/observations.csv', index=False)



# Transform Patients Data
def transform_patients():
    with open(f'{raw_path}/patients.json',"r") as f:
        patients_raw_data = json.load(f)
    patients = patients_data(patients_raw_data)
    patients.to_csv(f'{processed_path}/patients.csv', index=False)


# Transform Conditions Data
def transform_conditions():
    with open(f'{raw_path}/conditions.json',"r") as f:
        conditions_raw_data = json.load(f)
    Conditions = conditions_data(conditions_raw_data)
    Conditions.to_csv(f'{processed_path}/conditions.csv', index=False)