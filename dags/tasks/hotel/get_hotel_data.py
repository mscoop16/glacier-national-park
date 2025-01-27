import requests
from datetime import datetime, timedelta
from airflow.models import Variable
import json

# Hotel: The Lodge at Whitefish Lake
HOTEL_ID = 345614

TEMP_FILE = '/tmp/hotel_room.json'

API_URL = "https://booking-com15.p.rapidapi.com/api/v1/hotels/getRoomList"
API_KEY = Variable.get('booking-api-key')
RUN_DATE = datetime.now().strftime('%Y-%m-%d')
ARRIVAL_DATE = (datetime.now() + timedelta(weeks=1)).strftime('%Y-%m-%d')
DEPARTURE_DATE = (datetime.now() + timedelta(weeks=2)).strftime('%Y-%m-%d')
API_QUERY = {
    'hotel_id': HOTEL_ID,
    'arrival_date' : ARRIVAL_DATE,
    'departure_date': DEPARTURE_DATE,
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