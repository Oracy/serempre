import boto3
import hashlib
import json
import os
import pandas as pd
import re

from io import StringIO


# Environment Variables (set in the Fargate task)
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')  # Queue URL
OUTPUT_BUCKET_NAME = os.getenv('SQS_QUEUE_URL')  # Output S3 bucket
# SQS_QUEUE_URL = 'https://sqs.YOUR_REGION.amazonaws.com/794038211747/temperature_by_city'  # Queue URL
# OUTPUT_BUCKET_NAME = 'serempre-test'  # Output S3 bucket

# Use your actual AWS credentials here
sqs = boto3.client(
    'sqs',  # Service you are interacting with
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name='us-east-1'  # Specify the region
)

# SQS and S3 Clients
s3 = boto3.client(
    's3',
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name='us-east-1'
)


def split_files_into_countries_upload_to_s3(csv_file):
    # Insert data into s3
    countries = csv_file['Country'].unique()
    for country in countries:
        if country == "Côte D'Ivoire":
            country_file = csv_file[csv_file['Country'].isin([country])]
            country = re.sub(r"Côte D'Ivoire", "cote_divoire", country.lower())
        else:
            country_file = csv_file[csv_file['Country'].isin([country])]
            country = re.sub(r"\s", "_", country.lower())
        csv_buffer = StringIO()
        country_file.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket='serempre-test', Key=f"transformed/{country}_temperature_by_city", Body=csv_buffer.getvalue())


def hash_long_lat(csv_file):
    csv_file['Latitude'] = [hashlib.md5(str(val).encode('utf-8')).hexdigest() for val in csv_file['Latitude']]
    csv_file['Longitude'] = [hashlib.md5(str(val).encode('utf-8')).hexdigest() for val in csv_file['Longitude']]
    return csv_file


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a CSV file from S3 and load it into a Pandas DataFrame."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    file_content = obj['Body'].read().decode('utf-8')
    csv_buffer = StringIO(file_content)
    df = pd.read_csv(csv_buffer)
    return df


def move_handled_file(bucket_name, source_key, destination_key):    
    # Copy the file from the old location to the new location
    s3.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': source_key},
        Key=destination_key
    )
    
    # Delete the original file
    s3.delete_object(Bucket=bucket_name, Key=source_key)


def main():
    while True:
        # Receive messages from SQS
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1
        )
        messages = response.get('Messages', [])
        if not messages:
            print("No messages in the queue.")
            continue

        for message in messages:
            try:
                body = json.loads(message['Body'])
                records = body['Records'][0]
                bucket = records['s3']['bucket']['name']
                key = records['s3']['object']['key']
                destination_key = 'handled/GlobalLandTemperaturesByMajorCity.csv'

                # Download the file from S3
                df = read_csv_from_s3(bucket, key)

                # Process the CSV file
                df = hash_long_lat(df)
                split_files_into_countries_upload_to_s3(df)
                move_handled_file(bucket, key, destination_key)
                print("done")
                # Delete message from queue after processing
                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )
                print(f"Processed and deleted message: {message['MessageId']}")

            except Exception as e:
                print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()
