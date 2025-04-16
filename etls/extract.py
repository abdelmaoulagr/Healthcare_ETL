import requests
import json
from utils.constants import raw_path

def extract_fhir_data(url: str):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")