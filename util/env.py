""" AWS CREDENTIALS AS ENVIRONMENT VARIABLES """
import os
from dotenv import load_dotenv

load_dotenv()
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
