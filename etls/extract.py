import requests
import json
from utils.constants import raw_path

def extract_fhir_data():
    url='http://hapi.fhir.org/baseR4/Observation?code=8867-4'
    response = requests.get(url)
    if response.status_code == 200:
        # Create the directory if it doesn't exist
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        # Write the JSON data to the file
        with open(raw_path, "w") as f:
            json.dump(response.json(), f)
        print("Data extracted successfully!")
    else:
        raise Exception("API request failed")
