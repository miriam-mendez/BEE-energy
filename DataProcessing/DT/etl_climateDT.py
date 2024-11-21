import argparse
from extract import extract_data
from transform import transform_data
from load import upload_data
from datetime import datetime, timedelta
# Running example: python postgres.py -s 201501 -f 201502

def get_parser():
    parser = argparse.ArgumentParser(description="Process Climate DT data.")
    parser.add_argument("-s", "--start", 
                        help='Start date YYYYMMdd e.g. 20260101', type=str,
                        default='20260102')
    parser.add_argument("-f", "--final", 
                        help='Final date YYYYMMdd e.g. 20260131', type=str,
                        default='20260131')
    return parser


if __name__ == "__main__":
    """
    ERA5Land monthly ETL
    """
    parser = get_parser()
    args = parser.parse_args()    
    
    current_date = datetime.strptime(args.start,'%Y%m%d')
    end_date = datetime.strptime(args.final, '%Y%m%d')
    while current_date <= end_date:
        print(f"Harmonizing data from {current_date.strftime('%Y-%m-%d')}")
        date_str = current_date.strftime('%Y%m%d')
        print(f"Extracting data")
        bytes_data = extract_data("lumi","climate-dt",date_str)
        print(f"transforming data")
        df = transform_data(bytes_data)
        upload_data(df,table_name="climatedt")
    
        current_date += timedelta(days=1)
    

