import json
import pandas as pd
from dateutil import parser
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os


S3_CONN_ID = os.environ.get('S3_CONN_ID')
S3_KEY_TEMPLATE = "raw/flights/{{ ds }}/phl_to_mso.json"
BUCKET = os.environ.get('BUCKET')

def transform_flight_data(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_key = S3_KEY_TEMPLATE.replace("{{ ds }}", kwargs['ds'])
        raw_data = s3_hook.read_key(key=s3_key, bucket_name=BUCKET)
        data = json.loads(raw_data)

        flights = data.get('data', {})

        if 'error' in flights:
            print(f'Error returned by API: {flights['error']['code']}')
            return

        flight_deals = flights.get('flightDeals', [])
        best_flight_deal = next((flight for flight in flight_deals if flight['key'] == 'BEST'), None)
        if not best_flight_deal:
            raise ValueError("No 'best' flight deal found.")
        token = best_flight_deal['offerToken']

        flight_offers = flights.get('flightOffers', [])
        best_flight = next((flight for flight in flight_offers if flight['token'] == token), None)
        if not best_flight:
            raise ValueError("No matching 'best' flight offer found.")
        
        segment = best_flight['segments'][0]

        departure = segment['departureAirport']['code']
        arrival = segment['arrivalAirport']['code']
        total_price = best_flight['priceBreakdown']['total']['units'] # In USD per request

        legs = segment['legs']
        ordered_airports = []
        carriers = set()
        for leg in legs:
            if not ordered_airports or ordered_airports[-1] != leg['departureAirport']['code']:
                ordered_airports.append(leg['departureAirport']['code'])
            ordered_airports.append(leg['arrivalAirport']['code'])
            carriers.update(leg['carriers'])

        num_legs = len(legs)
        departure_dt = parser.isoparse(segment['departureTime'])
        arrival_dt = parser.isoparse(segment['arrivalTime'])
        flight_time = round((arrival_dt - departure_dt).total_seconds() / 3600, 2)

        flight_info = {
            'token': token,
            'total_price': total_price,
            'num_legs': num_legs,
            'departure': departure,
            'arrival': arrival,
            'legs': ordered_airports,
            'carriers': list(carriers),
            'flight_time': flight_time
        }

        df = pd.DataFrame([flight_info])
        df['legs'] = df['legs'].apply(json.dumps)
        df['carriers'] = df['carriers'].apply(json.dumps)
        temp_file = '/tmp/transformed_flight_data.csv'
        df.to_csv(temp_file, index=False)

        return temp_file

    except Exception as e:
        print(f'Error during transformation: {e}')
        raise