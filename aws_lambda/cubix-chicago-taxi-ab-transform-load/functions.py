import json
import pandas as pd
from typing import Dict, List
from io import StringIO

def read_file_from_s3(s3, bucket: str, key: str, file_format: str = "csv") :
	"""
    read a csv or json file from S3 bucket

    :param s3:              s3 client
    :param bucket:          name of the s3 bucket where the file is stored
    :param key:             path within the s3 bucket
    :param file_format:     json or csv
    """
	response = s3.get_object(Bucket = bucket, Key = key)
	content = response['Body'].read().decode('utf-8')

	if file_format == 'csv' :
		return pd.read_csv(StringIO(content))
	elif file_format == 'json' :
		return json.loads(content)
	else :
		raise ValueError(f"Unsupported file format: {file_format}")

def transform_weather(data: Dict) -> pd.DataFrame :
	"""
	select and transform the weather data

	:weather_data:      daily weather data from the API
	:return:            transformed weather dataframe
	"""
	weather_data = {
		"datetime" : data["hourly"]["time"],
		"temperature" : data["hourly"]["temperature_2m"],
		"wind_speed" : data["hourly"]["wind_speed_10m"],
		"rain" : data["hourly"]["rain"],
		"precipitation" : data["hourly"]["precipitation"]
	}

	weather_df = pd.DataFrame(weather_data)
	weather_df["datetime"] = pd.to_datetime(weather_df["datetime"])

	return weather_df

def transform_taxi(raw_taxi_data: List[Dict]) -> pd.DataFrame :
	"""
	1. drop selected columns
	2. drop null values
	3. rename selected columns
	4. create helper column for dim_weather join

	:param raw_taxi_data:   json file holding the daily taxi trips
	:raises TypeError:      if taxi_trips is not a dataframe
	:return:                transformed dataframe
	"""
	taxi_trips = pd.DataFrame(raw_taxi_data)

	taxi_trips.drop(['pickup_census_tract', 'dropoff_census_tract', 'pickup_centroid_location', 'dropoff_centroid_location'], axis = 1,
					inplace = True)
	taxi_trips.dropna(inplace = True)
	taxi_trips.rename(
			columns = {'pickup_community_area' : 'pickup_community_area_id', 'dropoff_community_area' : 'dropoff_community_area_id'},
			inplace = True)
	# időjárás adatok segéd oszlop - adott trip start timestamp órára lekerekített értéke
	taxi_trips['trip_start_timestamp'] = pd.to_datetime(taxi_trips['trip_start_timestamp'])
	taxi_trips['datetime_for_weather'] = taxi_trips['trip_start_timestamp'].dt.floor('h')  # órára lefelé kerekít
	return taxi_trips

def update_dim_tables(taxi_trips: pd.DataFrame, dim_df: pd.DataFrame, value_col: str) -> pd.DataFrame :
	"""
        extend the dimension df with new values if there are any

        :param taxi_trips: dataframe holding the daily taxi trips
        :param dim_df: dataframe with the dimension data
        :param value_col: name column of the dimension dataframe containing the values
        :return: updated dimension data, new values added if there is any new values in taxi trips data
        """
	id_col = f"{value_col}_id"

	todays_dim_data = pd.DataFrame(taxi_trips[value_col].unique(), columns = [value_col])
	new_dim_data = todays_dim_data[~todays_dim_data[value_col].isin(dim_df[value_col])]

	if not new_dim_data.empty :
		max_id = dim_df[id_col].max()
		new_dim_data[id_col] = range(max_id + 1, max_id + 1 + len(new_dim_data))
		dim_df = pd.concat([dim_df, new_dim_data], ignore_index = True)

	return dim_df

def update_fact_taxi_trips_with_dim_data(taxi_trips: pd.DataFrame, dim_payment_type: pd.DataFrame, dim_company: pd.DataFrame) -> pd.DataFrame :
	"""
	add the dimension data to the transformed taxi trips dataframe

	:taxi_trips: fact table taxi trips
	:dim_payment_type: dimension table for payment types
	:dim_company: dimension table for company
	:return: transformed taxi trips dataframe with payment type and company id
	"""
	fact_taxi_trips = taxi_trips.merge(dim_payment_type, on = "payment_type")
	fact_taxi_trips = fact_taxi_trips.merge(dim_company, on = "company")
	fact_taxi_trips.drop(["payment_type", "company"], axis = 1, inplace = True)

	return fact_taxi_trips

# function for inner use only
def _upload_df_to_s3(s3, bucket: str, df: pd.DataFrame, path = str) :
	"""
	upload a dataframe to specified s3 path
	"""
	buffer = StringIO()
	df.to_csv(buffer, index = False)
	df_content = buffer.getvalue()
	s3.put_object(Bucket = bucket, Key = path, Body = df_content)
	print("Uploaded dataframe to S3.")

# function for inner use only
def _move_file_on_s3(s3, bucket: str, source_key: str, target_key: str) :
	"""
	move a file within s3 by copying to a new location and deleting the original
	"""
	s3.copy_object(
			Bucket = bucket,
			CopySource = {"Bucket" : bucket, "Key" : source_key},
			Key = target_key
	)
	s3.delete_object(Bucket = bucket, Key = source_key)
	print(f"Archived raw data.")

def upload_dim_to_s3(s3, bucket: str, dim_type: str, df: pd.DataFrame) :
	"""
	upload a dimension table to specified s3 path

	:param s3:              s3 client
	:param bucket:          name of the s3 bucket where the file is stored
	:param dim_type:           name of the dimension table
	:param df:              dataframe to upload
	"""
	if not dim_type in ["company", "payment_type"] :
		raise ValueError("dim_type must be either 'company' or 'payment_type'")

	current_file_path = f"transformed_data/dim_{dim_type}/dim_{dim_type}.csv"
	previous_version_file_path = f"transformed_data/dimension_table_previous_versions/dim_{dim_type}.csv"

	s3.copy_object(
			Bucket = bucket,
			CopySource = {"Bucket" : bucket, "Key" : current_file_path},
			Key = previous_version_file_path
	)
	print(f"Copied existing version of {dim_type} to previous version folder")

	_upload_df_to_s3(s3, bucket, df, path = current_file_path)

def upload_and_archive_on_s3(s3, bucket: str, df: pd.DataFrame, file_type: str) :
	"""
	uploads a transformed dataframe to s3 and archives the corresponding raw file
	moves the original raw file from '/to_process' to '/processed'
	:param s3:              s3 client
	:param bucket:          name of the s3 bucket where the file is stored
	:param df:              dataframe to upload
	:param file_type:       type of the file to upload
	"""
	match file_type :
		case 'taxi' :
			# taxi_2024-01-01.csv
			formatted_date = df['datetime_for_weather'].dt.strftime('%Y-%m-%d').iloc[0]
			transformed_key = f'transformed_data/fact_taxi_trips/taxi_{formatted_date}.csv'
		case 'weather' :
			# weather_2024-01-01.csv
			formatted_date = df['datetime'].dt.strftime('%Y-%m-%d').iloc[0]
			transformed_key = f'transformed_data/dim_weather/weather_{formatted_date}.csv'
		case _ :  # anything non taxi or weather
			raise ValueError(f"file_type must be 'taxi' or 'weather'")

	# 1:upload transformed data
	_upload_df_to_s3(s3, bucket, df, transformed_key)

	# 2: move raw file to archive, delete original afterwards
	source_key = f'raw_data/to_process/{file_type}/{file_type}_{formatted_date}.json'
	target_key = f'raw_data/processed/{file_type}/{file_type}_{formatted_date}.json'
	_move_file_on_s3(s3, bucket, source_key, target_key)