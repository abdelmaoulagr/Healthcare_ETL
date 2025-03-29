import pandas as pd
import numpy
import json
from pathlib import Path


def transform_fhir_data():
    raw_path = Path("data/raw/raw_fhir_data.json")
    processed_path = Path("data/processed/transformed_data.csv")

    with open(raw_path,"r") as f:
        data = json.load(f)

    # Extract observations
    observations = data.get("entry",[])
    records =[]

    for obs in observations:
        resource = obs["resource"]
        patient_id = resource["subject"]["reference"].split("/")[-1]
        value = resource["valueQuantity"]["value"]
        unit = resource["valueQuantity"]["unit"]
        timestamp = resource["effectiveDateTime"]
        records.append({"patient_id": patient_id, "heart_rate": value,"unit": unit, "timestamp": timestamp})

    # # Convert to data frame
    df = pd.DataFrame(records)
    df["timestamp"]=pd.to_datetime(df["timestamp"])
    # df.dropna(inplace=True)
    print(df)

    # # save transformed data 
    # df.to_csv(processed_path, index=False)



# if __name__ == "__main__":
#     transform_fhir_data()    
