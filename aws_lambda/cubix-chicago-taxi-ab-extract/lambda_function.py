import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List

import boto3
import botocore
import requests


S3_BUCKET = 'cubix-chicago-taxi-ab'
TAXI_API_URL = "https://data.cityofchicago.org/resource/ajtu-isnz.json"
WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/era5"

s3_client = boto3.client('s3')


def get_data_from_api(url: str, params: Dict = None) -> List[Dict]:
    """
    Retrieves data from the given API with optional params
    :param url: the URL to retrieve data from
    :param params: optionally send parameters with the requests
    :return: a list of dictionaries containing the data
    """
    print(f'retrieving data from {url}')
    response = requests.get(url, params = params)
    return response.json()


def get_taxi_data(date_str: str) -> List[Dict]:
    """
    Retrieves taxi from the given date
    :param date_str: date to retrieve the data from
    :return: a list of dictionaries containing taxi data
    """
    print(f'retrieving taxi data for {date_str}')
    query = f"?$where=trip_start_timestamp >= '{date_str}T00:00:00' AND trip_start_timestamp <= '{date_str}T23:59:59'&$limit=30000"
    return get_data_from_api(TAXI_API_URL + query)


def get_weather_data(date_str: str) -> List[Dict]:
    """
    Retrieves weather data from the given date
    :param date_str: date to retrieve the data from
    :return: a list of dictionaries containing taxi data
    """
    print(f'retrieving weather data for {date_str}')
    params = {
        "latitude": 41.85,
        "longitude": -87.65,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": "temperature_2m,wind_speed_10m,rain,precipitation"
    }
    return get_data_from_api(WEATHER_API_URL, params)

def upload_to_s3(data: Dict, folder: str, filename: str) -> None:
    """
    Uploads the given data to S3
    :param data: data to upload
    :param key: S3 key to upload the data to
    :return: None
    """
    if not data:
        raise ValueError(f"Data for {filename} is empty! Stopping execution.")
    s3_key = f"raw_data/to_processed/{folder}/{filename}"
    try:
        s3_client.put_object(
            Body=json.dumps(data),
            Bucket=S3_BUCKET,
            Key=s3_key
        )
        print(f"Uploaded {filename} to S3.")
    except Expection as e:
        raise RuntimeError(f"Error uploading {filename} to S3: {e}") from e

def lambda_handler(event, context):
    """
    Lambda function to extract taxi and weather data from the given date and upload it to s3

    Steps:
    1. create the date (today minus 4 months)
    2. get the taxi raw data
    3. get the weather raw data
    4. upload data to s3
    """
    date_str = (datetime.now() - relativedelta(months=2)).strftime("%Y-%m-%d")

    taxi_data = get_taxi_data(date_str)
    weather_data = get_weather_data(date_str)

    upload_to_s3(taxi_data, "taxi", f"taxi_{date_str}.json")
    upload_to_s3(weather_data, "weather", f"weather_{date_str}.json")
