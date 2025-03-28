import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from etls.extract import extract_fhir_data

extract_fhir_data()

