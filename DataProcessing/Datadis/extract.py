import boto3
import yaml
from tqdm import tqdm

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


def get_s3_objects(s3,bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix = prefix)
    if 'Contents' not in response: return print("Data not found.")
    objects = [obj["Key"] for obj in response['Contents']]
    bytes_data = [s3.get_object(Bucket=bucket, Key=key)['Body'].read() for key in tqdm(objects, desc="Retrieving BCN/Lleida/Girona/Tarragona data")]
    return dict(zip(objects, bytes_data))


def extract_data(site_name, bucket="era5-data", date="201501"):
    s3 = setup_S3(site_name)
    files = {}
    for x in ['Barcelona','Girona','Lleida','Tarragona']:
            prefix = f"datadis/{x}/{date[:4]}-{date[4:]}/"
            files.update(get_s3_objects(s3,bucket, prefix))
    return files
    