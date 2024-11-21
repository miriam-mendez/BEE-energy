import boto3
import yaml


with open('/home/eouser/Desktop/DEDL/credentials.yaml', 'r') as f:
    credentials = yaml.safe_load(f)


def setup_S3(site_name):
	S3_URL = f"https://s3.{site_name}.data.destination-earth.eu"
	session = boto3.Session(
			aws_access_key_id=credentials[site_name]["key"],
			aws_secret_access_key=credentials[site_name]["secret"],
			region_name=credentials[site_name]["region"],
	)
	return session.client('s3', endpoint_url=S3_URL)


def extract_data(site_name, bucket="era5-data", date="201501"):
    s3 = setup_S3(site_name)
    response = s3.list_objects_v2(Bucket=bucket)
    
    objects = [obj["Key"] for obj in response['Contents'] if date in obj["Key"]]
    bytes_data =  [s3.get_object(Bucket=bucket, Key=obj)['Body'].read() for obj in objects]
    return dict(zip(objects, bytes_data))
    
    