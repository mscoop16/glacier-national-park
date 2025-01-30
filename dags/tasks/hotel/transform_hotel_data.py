import polars as pl
from datetime import datetime, timedelta
from io import StringIO
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


S3_CONN_ID = 'aws_default'
S3_TRANSFORMED_KEY_TEMPLATE = "transformed/hotels/{{ ds }}/transformed_hotels.csv"
S3_KEY_TEMPLATE = "raw/hotels/{{ ds }}/best_hotels.json"
BUCKET = 'glacier-national-park'

RUN_DATE = datetime.now().strftime('%Y-%m-%d')
CHECK_IN_DATE = (datetime.now() + timedelta(weeks=1)).strftime('%Y-%m-%d')
CHECK_OUT_DATE = (datetime.now() + timedelta(weeks=2)).strftime('%Y-%m-%d')

def transform_hotel_data(**kwargs):
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_key = S3_KEY_TEMPLATE.replace("{{ ds }}", kwargs['ds'])
        raw_data = s3_hook.read_key(key=s3_key, bucket_name=BUCKET)
        data = json.loads(raw_data)

        hotels = data.get('data', {}).get('hotels', [])

        hotel_data = []

        for hotel in hotels:
            prop = hotel['property']

            name = prop['name']
            hotel_id = hotel['hotel_id']
            latitude = prop['latitude']
            longitude = prop['longitude']
            price = prop['priceBreakdown']['grossPrice']['value']

            reviewCount = prop['reviewCount']
            reviewScore = prop['reviewScore']
            reviewScoreWord = prop['reviewScoreWord']

            hotel_data.append((name, hotel_id, price, latitude, longitude, reviewCount, reviewScore, reviewScoreWord))

        df = pl.DataFrame(
            data=hotel_data,
            schema=['name', 'hotel_id', 'price', 'latitude', 'longitude', 'reviewCount', 'reviewScore', 'reviewScoreWord'],
            orient='row'
        )

        # Handle missing values
        df = df.with_columns(
            pl.col('reviewCount').fill_null(strategy='zero').alias('reviewCount'),
            pl.col('reviewScore').fill_null(strategy='zero').alias('reviewScore')
        )

        # Add column for weighted score
        df = df.with_columns(
            ((pl.col('reviewScore') * pl.col('reviewCount')) / (pl.col('reviewCount') + 1)).alias('weightedScore')
        )

        # Add column with run date, check in date, and check out date
        df = df.with_columns(
            pl.lit(RUN_DATE).alias('RUN_DATE'),
            pl.lit(CHECK_IN_DATE).alias('CHECK_IN_DATE'),
            pl.lit(CHECK_OUT_DATE).alias('CHECK_OUT_DATE')
        )

        csv_buffer = StringIO()
        df.write_csv(csv_buffer)
        csv_buffer.seek(0)

        transformed_s3_key = S3_TRANSFORMED_KEY_TEMPLATE.replace("{{ ds }}", kwargs['ds'])

        # Upload CSV to S3
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=transformed_s3_key,
            bucket_name=BUCKET,
            replace=True
        )

        return f"s3://{BUCKET}/{transformed_s3_key}"

    except Exception as e:
        print(f'Error during transformation: {e}')
        raise