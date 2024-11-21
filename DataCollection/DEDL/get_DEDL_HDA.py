# Running example: python get_DEDL_HDA.py -o Data -d ClimateDT -s 20240501 -f 20240630 

import os
import argparse
import HDA
import yaml
from dateutil.relativedelta import relativedelta
from datetime import datetime

import HDA.climateDT
import HDA.era5
import HDA.extremeDT


# Variable to set by the user
BBOX = [0.1,40.5,3.4,42.9] # Catalonia bounding box


DATASETS = {
    'ClimateDT': {'query': HDA.climateDT.query, 'collections': HDA.climateDT.collection, 'bbox':BBOX},
    'ExtremeDT': {'query': HDA.extremeDT.query, 'collections': HDA.extremeDT.collection, 'bbox':BBOX},
    'ERA5Land': {'query': HDA.era5.query, 'collections': HDA.era5.collection, 'bbox':BBOX},
}

def get_parser():
    parser = argparse.ArgumentParser(description="Download and process climate data.")
    parser.add_argument("-o", "--output", 
                        help="Directory path to store the grib files", type=str, 
                        default="/energycat/ERA5")
    parser.add_argument("-d", "--dataset", 
                        nargs="?",type=str,
                        help='Download data from ClimateDT, ExtremeDT or ERA5Land',
                        choices = ['ClimateDT', 'ExtremeDT', 'ERA5Land'],
                        default='ERA5Land')
    parser.add_argument("-s", "--start", 
                        help='Start date YYYYMM for ERA5 and YYYYYMMDD for ClimateDT and ExtremeDT e.g. 20240404', type=str,
                        default='202404')
    parser.add_argument("-f", "--final", 
                        help='Final date YYYYMM for ERA5 and YYYYYMMDD for ClimateDT and ExtremeDT e.g. 20240404', type=str,
                        default='202410')
    return parser


def fetch_era5land_data(client, filter,  output_dir, start_date, end_date):
    ym = start_date
    while ym <= end_date:
        year, month = int(ym[:4]), int(ym[4:6])
        print(f"Downloading {year}-{month:02} in bounding box: {BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]}")
        filename = f'{output_dir}/{year}{month:02}_{BBOX[3]}_{BBOX[0]}_{BBOX[1]}_{BBOX[2]}.grib'
        if not os.path.exists(filename):
            filter.update({"year": {'eq': str(year)}, "month": {'eq': f'{month:02}'}})
            client.retrieve(filter, filename=filename)
        ym = (datetime.strptime(ym, '%Y%m') + relativedelta(months=1)).strftime('%Y%m')


def fetch_DT_data(client, filter, output_dir, start_date, end_date):
    current_date = datetime.strptime(start_date, '%Y%m%d')
    end_date = datetime.strptime(end_date, '%Y%m%d')
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%dT00:00:00Z")
        print(f"Downloading {date_str} in bounding box: {BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]}")
        filename = f"{output_dir}/{date_str}_{BBOX[3]}_{BBOX[0]}_{BBOX[1]}_{BBOX[2]}.grib"
        if not os.path.exists(filename):
            client.retrieve( filter, datechoice=date_str, filename=filename)
        current_date += relativedelta(days=1)


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    
    # Get client
    with open('./credentials.yaml', 'r') as f:
        credentials = yaml.safe_load(f)
    client = HDA.Client(credentials['username'],credentials['password'])
   
    # Set the environment
    os.makedirs(args.output,exist_ok=True)
    DATASETS[args.dataset]['query']['bbox'] = {'eq': BBOX}

    # Download data
    if args.dataset == 'ERA5Land':
        # download monthly data
        fetch_era5land_data(client, DATASETS[args.dataset], args.output, args.start, args.final)
    else:
        # download daily data
        fetch_DT_data(client, DATASETS[args.dataset], args.output, args.start, args.final)