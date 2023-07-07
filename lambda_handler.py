import io
import boto3
import pandas as pd

from util.env import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def lambda_handler(event, context):
    # print(f'Lambda event: {event}')

    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    client = boto3.client('s3',
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                          )

    obj = client.get_object(Bucket=bucket, Key=object_key)
    generator = obj['Body'].iter_lines()

    headers = next(generator).decode('utf-8').split(',')
    # initialize dictionary
    empty_count = {}
    for h in headers:
        empty_count[h] = 0
    # count empty cells
    for line in generator:
        row = line.decode('utf-8').split(',')
        for idx, value in enumerate(row):
            if not value:
                empty_count[headers[idx]] += 1

    print(f'Successfully count empty cells for each column: {empty_count}')

    return {
        'statusCode': 200,
        'body': [empty_count]
    }


def config_event():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "miazatobucket",
                    },
                    "object": {
                        "key": "mock_empty_register.csv",
                    }
                }
            }
        ]
    }


lambda_handler(config_event(), {})
