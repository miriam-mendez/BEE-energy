import argparse
from extract import extract_data
from transform import transform_data
from load import upload_data

# Running example: python postgres.py -s 201501 -f 201502

def get_parser():
    parser = argparse.ArgumentParser(description="Process ERA5 data.")
    parser.add_argument("-s", "--start", 
                        help='Start date YYYYMM e.g. 202405', type=str,
                        default='202404')
    parser.add_argument("-f", "--final", 
                        help='Final date YYYYMM e.g. 202404', type=str,
                        default='202409')
    return parser


if __name__ == "__main__":
    """
    ERA5Land monthly ETL
    """
    parser = get_parser()
    args = parser.parse_args()    
    
    current_date = args.start
    # while int(current_date) <= int(args.final):
    print(f"Harmonizing data from {current_date}")
    
    bytes_data = extract_data("lumi","era5-data","destine/202410_42.9_0.1_40.5_3.4.grib")
    print(len(bytes_data))
    df = transform_data(bytes_data)
    upload_data(df,table_name="era5")

    month = int(current_date[4:])%12 + 1
    year = int(current_date[:4]) + int(current_date[4:])//12
    current_date = f"{year}{month:02d}"
    

