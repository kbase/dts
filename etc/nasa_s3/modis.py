# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "boto3",
#     "requests"
# ]
# ///
import boto3
import requests
from getpass import getpass

user = getpass(prompt='Enter NASA Earthdata Login Username: ')
password = getpass(prompt='Enter NASA Earthdata Login Password: ')

url = 'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials'
url = requests.get(url, allow_redirects=False).headers['Location']
creds = requests.get(url, auth=(user, password)).json()

print(creds)

session = boto3.Session(
    aws_access_key_id=creds['accessKeyId'],
    aws_secret_access_key=creds['secretAccessKey'],
    aws_session_token=creds['sessionToken'],
    region_name='us-west-2')
client = session.client('s3')
bucket = 'lp-prod-protected'
prefix = 'MOD09GA.061/'
delimiter = '/'

bucket_list = client.list_objects_v2(
    Bucket=bucket,
    Prefix=prefix,
    Delimiter=delimiter)
bucket_list