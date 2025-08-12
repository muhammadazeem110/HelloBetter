import json
import boto3
import pandas as pd
from io import StringIO
import requests
import os

def make_api_calls(location, date, time, cache):

    cache_key = (location, date, time)
    if cache_key in cache:
        return None
    
    base_url = "https://api.weatherapi.com/v1/history.json"
    params = {
        "q" : location,
        "dt" : date,
        "hour" : time,
        "key" : os.environ["Weather_api_key"]
    }
    headers = {
        "accept" : "application/json"
    }
    try:
        api_response = requests.get(base_url, params=params, headers=headers)
        api_response.raise_for_status()
        response_body = api_response.json()
        weather = response_body["forecast"]["forecastday"][0]["hour"][0]["condition"]["text"]
        cache[cache_key] = weather
        return weather

    except Exception as error:
        print(f"Api call failed: {error}")
        return None

def lambda_handler(event, context):
    s3_client = boto3.client('s3')

    if 'Records' in event:
        for record in event['Records']:
            if 's3' in record:
                bucket_name = record['s3']['bucket']['name']
                key = record['s3']['object']['key']

                try:
                    response = s3_client.get_object(Bucket=bucket_name, Key=key)
                    chat_content = response['Body'].read().decode('utf-8')
                    df = pd.read_csv(StringIO(chat_content))
                    print(f"\nObject: {key}")
                    # print(df.head(3))

                    cache = {}
                    df['timestamp'] = pd.to_datetime(df['timestamp'])

                    api_df = pd.DataFrame()
                    api_df["weather"]  = pd.DataFrame(df.apply(lambda row: (make_api_calls(
                        row['user_location'],
                        row['timestamp'].strftime("%Y-%m-%d"),
                        row['timestamp'].hour,
                        cache
                    )),
                    axis = 1)
                    )
                    api_df["Location"] = df["user_location"]
                    api_df.dropna(inplace=True)

                    # Split the name of files to get the date's part
                    prefix, obj_key = os.path.split(key)
                    date_str = obj_key.rsplit("_", 1)[-1]


                    # Create an in-memory CSV (using StringIO) to store the DataFrame without saving a physical file.
                    # Then use csv_buffer.getvalue() to get its string contents and upload directly to S3.
                    csv_buffer = StringIO()
                    api_df.to_csv(csv_buffer, index=False)
                    s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/weather_{date_str}", Body=csv_buffer.getvalue())
                    
                    print(f"File '{obj_key}' uploaded successfully to '{bucket_name}'")

                except Exception as error:
                    print(f"Error processing : {error}")
            else:
                print("The event is not triggered by S3.")
    else:
        print("This lambda function expects a trigger.")
    
    return {
        "StatusCode" : 200,
        "body": "Files uploaded successfully!"
    }