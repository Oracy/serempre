import json
import boto3

# Initializing SQS and S3 clients
sqs_client = boto3.client('sqs')
queue_url = "https://sqs.us-east-1.amazonaws.com/794038211747/temperature_by_city"

def lambda_handler(event, context):
    """
    Lambda function to handle s3 events and add message to SQS queue
    """
    try:
        # Extracting event information
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            file_key = record['s3']['object']['key']
            
            # Create dictionary with file information
            message = {
                "file": f"s3://{bucket_name}/{file_key}"
            }
            
            # Send message to SQS
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message)
            )
            
            print(f"Message sent to SQS: {response['MessageId']} - {message}")

        return {
            "statusCode": 200,
            "body": json.dumps("Success!")
        }
    
    except Exception as e:
        print(f"Erro: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps("Error!")
        }
