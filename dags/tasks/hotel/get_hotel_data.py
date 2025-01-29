import requests
from datetime import datetime, timedelta
from airflow.models import Variable
import json

# Area: Whitefish, MT
DESTINATION_ID = 20077564

TEMP_FILE = '/tmp/best_hotels.json'

API_URL = "https://booking-com15.p.rapidapi.com/api/v1/hotels/searchHotels"
API_KEY = Variable.get('booking-api-key')
RUN_DATE = datetime.now().strftime('%Y-%m-%d')
ARRIVAL_DATE = (datetime.now() + timedelta(weeks=1)).strftime('%Y-%m-%d')
DEPARTURE_DATE = (datetime.now() + timedelta(weeks=2)).strftime('%Y-%m-%d')
API_QUERY = {
    'dest_id': DESTINATION_ID,
    'search_type': 'city',
    'arrival_date' : ARRIVAL_DATE,
    'departure_date': DEPARTURE_DATE,
    'page_number': 1,
    'sort_by': 'class_descending',
    'adults': 2,
    'room_qty': 1,
    'currency_code' : 'USD'
}
API_HEADERS = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "booking-com15.p.rapidapi.com"
    }

def fetch_hotel_data(**kwargs):
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