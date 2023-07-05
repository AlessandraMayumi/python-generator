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
    body_bytes = obj['Body'].read()
    body_io = io.BytesIO(body_bytes)

    df = pd.read_csv(body_io)
    empty_count = df.isna().sum().to_json()
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
