import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))


# File Paths
INPUT_PATH = parser.get('file_paths', 'raw_path')
OUTPUT_PATH = parser.get('file_paths', 'processed_path')