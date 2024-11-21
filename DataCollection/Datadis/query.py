import os
import pandas as pd
import yaml
from datetime import date
from dateutil.relativedelta import relativedelta
import concurrent.futures
from tqdm import tqdm
import boto3
import datadis
from botocore.exceptions import ClientError

# Constants
BUCKET = "energy-storage"
INPUT_POSTALCODES_PATH = "catalonia_postalcodes.csv"  # src: https://analisi.transparenciacatalunya.cat/Urbanisme-infraestructures/Codis-postals-per-municipis-de-Catalunya/tp8v-a58g/data
OUTPUT_PATH = "datadis"
URL_REQUEST = 'https://datadis.es/api-public/api-search'
START_DATE = date(2023, 5, 1)
END_DATE = date(2024, 6, 30)

# Province codes mapping
PROVINCES_DICT = {
    '08': "Barcelona",
    '43': "Tarragona",
    '17': "Girona",
    '25': "Lleida",
    '22': "Lleida",
}

# Load Catalonia postal codes
df = pd.read_csv(INPUT_POSTALCODES_PATH, dtype=str)
df = df[df['Codi postal'].str[:2].isin(PROVINCES_DICT.keys())]
postal_codes = df["Codi postal"]

# Load user credentials to access datadis platform
with open('./credentials.yaml', 'r') as f:
    credentials = yaml.safe_load(f)

# Initialize datadis client
c = datadis.Client(credentials['user'], credentials['password'], storage=boto3.client("s3"))

def object_exists(bucket_name, object_key, s3_client):
    """Check if an object exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise e

def fetch_data(postal_code, fetch_date, bucket=None):
    """Fetch data for a given postal code and date, storing it locally or in S3."""
    year_month_dir = f"{OUTPUT_PATH}/{PROVINCES_DICT[postal_code[:2]]}/{str(fetch_date)[:7]}"
    file_path = f"{year_month_dir}/consumption_{postal_code}.csv"

    if (bucket and not object_exists(bucket, file_path, c.storage)) or (not bucket and not os.path.exists(file_path)):
        c.retrieve(
            URL_REQUEST,
            {
                'startDate': fetch_date.strftime('%Y/%m/%d'),
                'endDate': (fetch_date + relativedelta(months=1) - relativedelta(days=1)).strftime('%Y/%m/%d'),
                'page': 0,
                'economicSector': ['1', '2', '3', '4'],
                'timeDiscrimination': ['G0', 'E3', 'E2', 'E1'],
                'fare': ['30', '31', '3T', '3V', '62', '63', '64', '65', '21', '2A', '2T', '6A', '6V'],
                'community': '09',
                'postalCode': postal_code,
                'pageSize': 2000
            },
            target=file_path,
            bucket=bucket
        )

def download_data_for_date(current_date, bucket):
    """Download data for all postal codes for a specific date."""
    print(f'\nDownloading DATADIS consumption data from {str(current_date)[:7]}.')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        list(tqdm(executor.map(lambda x: fetch_data(x, current_date, bucket), postal_codes), total=len(postal_codes)))

def main():
    current_date = START_DATE
    while current_date < END_DATE:
        download_data_for_date(current_date, BUCKET)
        current_date += relativedelta(months=1)

if __name__ == "__main__":
    main()
