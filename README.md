# python-generator
Study python generator to stream data

## Run aws lambda
Create a `.env` file containing aws credentials
```shell
AWS_ACCESS_KEY_ID=AKI...
AWS_SECRET_ACCESS_KEY=niK...
```
Then call the lambda handler
```shell
pyhon lambda_handler.py
```

## Test csv files
The test csv file is stored in the folder `/data`
```shell
cd util/
python generate_csv.py
```