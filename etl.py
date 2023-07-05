import pandas as pd
from google.cloud import bigquery
import requests
import io

def run_uber_etl():
    # df= pd.read_csv("uber_data.csv")

    url = 'https://storage.googleapis.com/uber-etl-pipeline-aastha/uber_data.csv'
    response = requests.get(url)
    df= pd.read_csv(io.StringIO(response.text),sep=',')

    print(df.head())
    print(df.columns)

    # checking data types
    print(df.dtypes)

    # changing date columns to datetime value
    df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)

    # checking if VendorID can be used for primary key
    print(df['VendorID'].nunique())

    # VendorID not unique so have to create own primary key
    df['trip_id'] = df.index

    # creating the datetime dimension table
    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim['pick_hour'] = df['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = df['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = df['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = df['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = df['tpep_pickup_datetime'].dt.weekday
    datetime_dim['dropoff_hour'] = df['tpep_dropoff_datetime'].dt.hour
    datetime_dim['dropoff_day'] = df['tpep_dropoff_datetime'].dt.day
    datetime_dim['dropoff_month'] = df['tpep_dropoff_datetime'].dt.month
    datetime_dim['dropoff_year'] = df['tpep_dropoff_datetime'].dt.year
    datetime_dim['dropoff_weekday'] = df['tpep_dropoff_datetime'].dt.weekday

    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 
                                'pick_weekday','tpep_dropoff_datetime', 'dropoff_hour', 'dropoff_day', 'dropoff_month', 
                                'dropoff_year', 'dropoff_weekday']]
    print(datetime_dim.head())

    # creating the passenger_count dimension table
    passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

    print(passenger_count_dim.head())

    # creating the trip_distance dimension table
    trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]

    print(trip_distance_dim.head())

    # creating the rate_code dimension table
    rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dictionary = {1: "Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
    }

    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_dictionary)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]

    print(rate_code_dim.head())

    # creating the pickup location dim dimension table
    pickup_location_dim = df[['pickup_longitude','pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_longitude','pickup_latitude']]

    print(pickup_location_dim.head())

    # creating the dropoff location dimension table
    dropoff_location_dim = df[['dropoff_longitude','dropoff_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_longitude','dropoff_latitude']]

    print(dropoff_location_dim.head())

    # creating the payment_type dimension table
    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }

    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]


    # fact table

    fact_table = df.merge(datetime_dim,left_on='trip_id',right_on='datetime_id')\
                    .merge(passenger_count_dim,left_on='trip_id',right_on='passenger_count_id')\
                    .merge(trip_distance_dim,left_on='trip_id',right_on='trip_distance_id')\
                    .merge(rate_code_dim,left_on='trip_id',right_on='rate_code_id')\
                    .merge(pickup_location_dim,left_on='trip_id',right_on='pickup_location_id')\
                    .merge(dropoff_location_dim,left_on='trip_id',right_on='dropoff_location_id')\
                    .merge(payment_type_dim,left_on='trip_id',right_on='payment_type_id')\
                    [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
                            'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
                            'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                            'improvement_surcharge', 'total_amount']]
    
    # loading fact table
    service_account_file_path = "/home/aasthajha123/airflow/uber_dags/keys.json" # your service account auth file file
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = 'uber-etl-pipeline.uber_data_engineering.fact_table '

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = bigquery_client.load_table_from_dataframe(
    fact_table, table_id, job_config=job_config
    )

    #loading datetime_dim
    service_account_file_path = "/home/aasthajha123/airflow/uber_dags/keys.json" # your service account auth file file
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = 'uber-etl-pipeline.uber_data_engineering.datetime_dim '

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = bigquery_client.load_table_from_dataframe(
    datetime_dim, table_id, job_config=job_config
    )

    #loading passenger_count_dim
    service_account_file_path = "/home/aasthajha123/airflow/uber_dags/keys.json" # your service account auth file file
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = 'uber-etl-pipeline.uber_data_engineering.passenger_count_dim '

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = bigquery_client.load_table_from_dataframe(
    passenger_count_dim, table_id, job_config=job_config
    )

    #loading trip_distance_dim
    service_account_file_path = "/home/aasthajha123/airflow/uber_dags/keys.json" # your service account auth file file
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = 'uber-etl-pipeline.uber_data_engineering.trip_distance_dim '

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = bigquery_client.load_table_from_dataframe(
    trip_distance_dim, table_id, job_config=job_config
    )

    #loading rate_code_dim
    service_account_file_path = "/home/aasthajha123/airflow/uber_dags/keys.json" # your service account auth file file
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = 'uber-etl-pipeline.uber_data_engineering.rate_code_dim '

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = bigquery_client.load_table_from_dataframe(
    rate_code_dim, table_id, job_config=job_config
    )

    #loading pickup_location_dim
    service_account_file_path = "/home/aasthajha123/airflow/uber_dags/keys.json" # your service account auth file file
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = 'uber-etl-pipeline.uber_data_engineering.pickup_location_dim '

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = bigquery_client.load_table_from_dataframe(
    pickup_location_dim, table_id, job_config=job_config
    )

    #loading dropoff_location_dim
    service_account_file_path = "/home/aasthajha123/airflow/uber_dags/keys.json" # your service account auth file file
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = 'uber-etl-pipeline.uber_data_engineering.dropoff_location_dim '

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = bigquery_client.load_table_from_dataframe(
    dropoff_location_dim, table_id, job_config=job_config
    )

    #loading payment_type_dim
    service_account_file_path = "/home/aasthajha123/airflow/uber_dags/keys.json" # your service account auth file file
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = 'uber-etl-pipeline.uber_data_engineering.payment_type_dim '

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = bigquery_client.load_table_from_dataframe(
    payment_type_dim, table_id, job_config=job_config
    )



    # job.result()  # Waits for the job to complete.

    