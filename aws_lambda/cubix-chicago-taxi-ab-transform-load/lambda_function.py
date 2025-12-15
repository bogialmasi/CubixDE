import json
import boto3
import pandas as pd
from typing import Dict

from configs import (
	BUCKET,
	RAW_WEATHER_FOLDER,
	RAW_TAXI_FOLDER,
	DIM_PAYMENT_TYPE_PATH,
	DIM_COMPANY_PATH
)

from functions import (
	read_file_from_s3,
	transform_weather,
	transform_taxi,
	update_dim_tables,
	update_fact_taxi_trips_with_dim_data,
	upload_dim_to_s3,
	upload_and_archive_on_s3
)

def process_taxi_data(s3, dim_payment_type: pd.DataFrame, dim_company: pd.DataFrame) :
	"""
	process and transform daily taxi data
	1. download the raw taxi data
	2. read and transform
	3. updatr the payment type and company dim tables
	4. update taxi data with payment type and company ids
	5. upload the payment type and company to s3
	6. upload and archive taxi data
	:param s3:                  S3 client
	:param dim_payment_type:    payment type dimension table
	:param dim_company:         company dimension table
	"""
	for file in (s3.list_objects(Bucket = BUCKET, Prefix = RAW_TAXI_FOLDER)['Contents']) :
		taxi_key = file['Key']
		taxi_raw_file_name = file['Key'].split('/')[-1]  # .json kiterjesztés szűrése

		if taxi_raw_file_name.split('.')[-1] == "json" :
			taxi_raw_content = read_file_from_s3(s3, BUCKET, taxi_key, 'json')
			fact_taxi_trips = transform_taxi(taxi_raw_content)

			dim_company_updated = update_dim_tables(fact_taxi_trips, dim_company, 'company')
			dim_payment_type_updated = update_dim_tables(fact_taxi_trips, dim_payment_type, 'payment_type')

			fact_taxi_trips_updated = update_fact_taxi_trips_with_dim_data(fact_taxi_trips, dim_payment_type_updated, dim_company_updated)
			upload_dim_to_s3(s3, BUCKET, 'payment_type', dim_payment_type_updated)
			upload_dim_to_s3(s3, BUCKET, 'company', dim_company_updated)

			upload_and_archive_on_s3(s3, BUCKET, fact_taxi_trips_updated, 'taxi')

def process_weather_data(s3) :
	"""
	process and transform daily weather data
	1. download the raw weather data
	2. read and transform
	3. upload and archive the files
	"""
	for file in (s3.list_objects_v2(Bucket = BUCKET, Prefix = RAW_WEATHER_FOLDER)['Contents']) :
		weather_key = file['Key']
		weather_raw_file_name = file['Key'].split('/')[-1]  # .json kiterjesztés szűrése

		if weather_raw_file_name.split('.')[-1] == "json" :
			weather_raw_content = read_file_from_s3(s3, BUCKET, weather_key, 'json')
			dim_weather = transform_weather(weather_raw_content)

			upload_and_archive_on_s3(s3, BUCKET, dim_weather, 'weather')

def lambda_handler(event, context) :
	s3 = boto3.client('s3')
	print(s3)
	dim_payment_type = read_file_from_s3(s3, BUCKET, DIM_PAYMENT_TYPE_PATH, 'csv')
	dim_company = read_file_from_s3(s3, BUCKET, DIM_COMPANY_PATH, 'csv')

	process_taxi_data(s3, dim_payment_type, dim_company)
	process_weather_data(s3)
	print('all files processed.')