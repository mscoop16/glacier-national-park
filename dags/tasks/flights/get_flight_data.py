import requests
import json
from datetime import datetime, timedelta
from airflow.models import Variable

TEMP_FILE = '/tmp/phl_to_mso.json'

FLIGHT_DATE = (datetime.now() + timedelta(weeks=1)).strftime('%Y-%m-%d')
API_URL = "https://booking-com15.p.rapidapi.com/api/v1/flights/searchFlights"
API_KEY = Variable.get('booking-api-key')
API_QUERY = {"fromId":"PHL.AIRPORT",
                   "toId":"MSO.AIRPORT",
                   "departDate": FLIGHT_DATE,
                   "pageNo":"1",
                   "adults":"1",
                   "sort":"BEST",
                   "cabinClass":"ECONOMY",
                   "currency_code":"USD"}
API_HEADERS = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "booking-com15.p.rapidapi.com"
    }

def fetch_flight_data(**kwargs):
    try:
        response = requests.get(API_URL, headers=API_HEADERS, params=API_QUERY)
        response.raise_for_status()
        data = response.json()
        
        with open(TEMP_FILE, 'w') as f:
            json.dump(data, f)
        print(f'Data successfully written to temp file {TEMP_FILE}')
    except Exception as e:
        print(f'Error during API call: {e}')
        raise