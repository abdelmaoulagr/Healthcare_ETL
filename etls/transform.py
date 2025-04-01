import pandas as pd
import numpy
import json
from datetime import datetime
from utils.constants import raw_path, processed_path

def transform_fhir_data():
    with open(raw_path,"r") as f:
        data = json.load(f)

    # Extract observations
    observations = data.get("entry",[])
    records =[]

    for obs in observations:
        resource = obs["resource"]
        try:
            patient_id = resource["subject"]["reference"].split("/")[-1]
            value = resource["valueQuantity"]["value"]
            unit = resource["valueQuantity"]["unit"]
            timestamp = resource["effectiveDateTime"]
            # Append valid record to the list
            records.append(
                {
                "patient_id": patient_id,
                "heart_rate": value,
                "unit": unit,
                "timestamp": timestamp
                })

        except:
            continue

    # Convert to data frame
    df = pd.DataFrame(records)
    df["timestamp"]=pd.to_datetime(df["timestamp"], errors="coerce")
    df.dropna(inplace=True)
    print(df)
    # save transformed data 
    df.to_csv(processed_path, index=False)



# if __name__ == "__main__":
#     transform_fhir_data()    
