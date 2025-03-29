import os
import sys

from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transform import transform_fhir_data

transform_fhir_data()
