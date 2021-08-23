import logging
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()

s3 = boto3.client('s3',
  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

dir = './output/postcodes'

for filename in os.listdir(dir):
  if filename.endswith(".json"):
    with open(dir + '/' + filename, 'rb') as f:
      
      s3.upload_fileobj(
        f,
        "locobss-story-risk",
        "postcode/" + filename.split('.')[0] + '.json',
        ExtraArgs = {
          'ContentType': 'application/json',
          'ContentEncoding': 'gzip'
        }
      )
