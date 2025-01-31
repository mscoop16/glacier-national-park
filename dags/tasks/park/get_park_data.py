import requests
import json
from airflow.models import Variable

PARK_INFO_TEMP_FILE = '/tmp/glac_park_info.json'
THINGSTODO_TEMP_FILE = '/tmp/glac_thingstodo.json'

API_URL_INFO = "https://developer.nps.gov/api/v1/parks"
API_URL_THINGSTODO = "https://developer.nps.gov/api/v1/thingstodo"
API_KEY = Variable.get('booking-api-key')
API_QUERY = {
    'api_key': API_KEY,
    'parkCode': ['glac']
}

def fetch_park_data(**kwargs):
    try:
        response = requests.get(API_URL_INFO, params=API_QUERY)
        response.raise_for_status()
        data = response.json()
        
        with open(PARK_INFO_TEMP_FILE, 'w') as f:
            json.dump(data, f)
        print(f'Data successfully written to temp file {PARK_INFO_TEMP_FILE}')
    except Exception as e:
        print(f'Error during API call: {e}')
        raise

def fetch_park_activity_data(**kwargs):
    try:
        response = requests.get(API_URL_THINGSTODO, params=API_QUERY)
        response.raise_for_status()
        data = response.json()
        
        with open(THINGSTODO_TEMP_FILE, 'w') as f:
            json.dump(data, f)
        print(f'Data successfully written to temp file {THINGSTODO_TEMP_FILE}')
    except Exception as e:
        print(f'Error during API call: {e}')
        raise