import csv
import io
import boto3
import pandas as pd

from util.env import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def _get_csv_chunk(stream_body):
    columns = []
    pending = ''
    for chunk in stream_body.iter_chunks(chunk_size=1000000):
        decoded = pending + chunk.decode('utf-8')
        rows_list_raw = decoded.splitlines()
        if decoded[-1] != '\n':
            pending = rows_list_raw[-1]
            rows_list = rows_list_raw[0:-1]
        else:
            pending = ''
            rows_list = rows_list_raw
        if not columns:
            columns = rows_list[0].split(',')
            rows_list = rows_list[1::]
        yield [row.split(',') for row in rows_list], columns


def lambda_handler(event, context):
    # print(f'Lambda event: {event}')

    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    client = boto3.client('s3',
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                          )

    obj = client.get_object(Bucket=bucket, Key=object_key)
    stream_body = obj['Body']

    empty_count = None
    for rows, columns in _get_csv_chunk(stream_body):
        df = pd.DataFrame(rows, columns=columns).replace('', None)
        empty_count_chunk = df.isna().sum().to_dict()
        if not empty_count:
            empty_count = empty_count_chunk
        for key in empty_count_chunk:
            empty_count[key] += empty_count_chunk[key]

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
                        "key": "mock_big_register.csv",
                    }
                }
            }
        ]
    }


lambda_handler(config_event(), {})
