import configparser
import os
from pathlib import Path

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))


# File Paths
raw_path = Path("data/raw/raw_fhir_data.json")
processed_path = Path("data/processed/transformed_data.csv")

