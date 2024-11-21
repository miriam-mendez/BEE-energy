import numpy as np
import pygrib
import pandas as pd
import polars as pl
import geopandas as gpd
from shapely.geometry import Point
import warnings
from scipy.interpolate import griddata
import pvlib
from tqdm import tqdm
from load import upload_data
import glob
import boto3
import yaml
from datetime import datetime
from botocore.exceptions import ClientError

postalcodes_path = '/home/eouser/Desktop/SpanishPostalCodes/Catalonia/postal_codes.shp'
variables = [

    {
        'paramId':134,
        'name':'Surface pressure',
        'shortName': 'sp',
        'era5_name': 'surfacePressure'
    },
    {
        'paramId':164,
        'name':'Total cloud cover',
        'shortName': 'tcc',
        'era5_name': 'totalCloudCover'
    },
    {
        'paramId':165,
        'name':'10 metre U wind component',
        'shortName': '10u',
        'era5_name': 'windSpeedEast'
    },
    {
        'paramId':166,
        'name':'10 metre V wind component',
        'shortName': '10v',
        'era5_name': 'windSpeedNorth'
    },
    {
        'paramId':167,
        'name': '2 metre temperature',
        'shortName': '2t',
        'era5_name': 'airTemperature'
    },
    {
        'paramId':168,
        'name': '2 metre dewpoint temperature',
        'shortName': '2d',
        'era5_name': 'dewAirTemperature'
    },
    {
        'paramId':169,
        'name': 'Surface short-wave (solar) radiation downwards',
        'shortName': 'ssrd',
        'era5_name': 'GHI'
    },
    {
        'paramId':175,
        'name': 'Surface long-wave (thermal) radiation downwards',
        'shortName': 'strd',
        'era5_name': 'thermalRadiation'
    }
]
parameters = [x['shortName'] for x in variables]

def grib2df(filename):
    
    bbox = filename[:filename.rfind('.')]
    bbox = bbox.split("_")[1:]
    bbox = [float(b) for b in bbox] 

    grbs = pygrib.open(filename)
    grb = grbs[len(grbs)]


    mssgs = len(grbs)+1

    lats = grb.latitudes
    lons = grb.longitudes

    lat_mask = (lats >= bbox[2]) & (lats <= bbox[0])
    lon_mask = (lons >= bbox[1]) & (lons <= bbox[3])
    
    mask = lat_mask & lon_mask

    df_ = pl.DataFrame()
    slc = len(parameters)   
    for i in tqdm(range(1, mssgs, slc), desc="Reading GRIB messages"):
        try:
            data = {
                'latitude': [],
                'longitude': [],
                'time': []
            }
                
            for grb in grbs[i:i+slc]:
                if grb.shortName in parameters:
                    filtered_values = grb.values[mask]
                    data_array = np.where(filtered_values == 9999, np.nan, filtered_values)
                    data.update({grb.shortName: data_array.flatten()})
                else:
                    print(f"{grb.shortName} is not in ERA5 dictionary")
            if i == mssgs - slc: 
                grb = grbs[mssgs-1]  
                if grb.shortName in parameters:
                    filtered_values = grb.values[mask]
                    data_array = np.where(filtered_values == 9999, np.nan, filtered_values)
                    data.update({grb.shortName: data_array.flatten()})
                else:
                    print(f"{grb.shortName} is not in ERA5 dictionary")
                    
            time2 = grb.validDate
            data['latitude'].extend(lats[mask].flatten())
            data['longitude'].extend(lons[mask].flatten())
            data['time'].extend([time2] * len(lons[mask].flatten()))
            df_ = pl.concat([df_, pl.DataFrame(data)])   

        except Exception as e:
            print(e)
            print(grb)
            print(f"Error in message {i}")
    return df_



def filter_locations(path_shapefile, df):
    gdf = gpd.read_file(path_shapefile)
    gdf = gdf.to_crs(epsg=4326)
    gdf = gdf.dissolve(by='COD_POSTAL')
    gdf = gdf.reset_index()
    latslons = df.unique(subset=['latitude', 'longitude'])['latitude','longitude']
    geometry = [Point(lon, lat) for lon, lat in zip(latslons['longitude'], latslons['latitude'])]
    geo_df = gpd.GeoDataFrame(latslons, geometry=geometry, crs='EPSG:4326')

    filtered_gdf = geo_df[geo_df.geometry.within(gdf.union_all())]
    filter = pl.DataFrame(filtered_gdf.drop(columns='geometry'))
    filter = filter.rename({'0':'latitude','1':'longitude'})

    df = df.join(filter, on=['latitude', 'longitude'], how='inner') 
    return df
   

def join_solar_features(df):
    dfp = df.to_pandas()
    location = pvlib.location.Location(
        latitude=df.select("latitude").unique().item(),
        longitude=df.select("longitude").unique().item())
    solar_df = location.get_solarposition(
        dfp['time'] + pd.Timedelta(minutes=30),
        pressure=dfp['surfacePressure'],
        temperature=dfp['airTemperature']).reset_index()
    dni = pvlib.irradiance.disc(
        ghi=dfp["GHI"],
        solar_zenith=solar_df['apparent_zenith'],
        datetime_or_doy=solar_df['time'].dt.dayofyear,
        pressure=dfp["surfacePressure"])
    rad_df = pvlib.irradiance.complete_irradiance(
        solar_zenith=solar_df['apparent_zenith'],
        ghi=dfp["GHI"],
        dni=dni["dni"],
        dhi=None).rename(columns={'ghi':'GHI','dni':'DNI','dhi':'DHI'})
    solar_df = solar_df.drop(['apparent_zenith', 'zenith', 'apparent_elevation', 'equation_of_time'], axis=1)
    solar_df = solar_df.rename(columns={'elevation': 'sunElevation', 'azimuth': 'sunAzimuth'})
    solar_df['time'] = (solar_df['time'] - pd.Timedelta(minutes=30)).astype('datetime64[us]')
    solar_df = pl.from_pandas(pd.concat([solar_df, rad_df], axis=1))
    return solar_df.join(df.drop("GHI"), on="time", how="inner")


def transform_features(df):
    df = df.sort(["latitude", "longitude", "time"])
    
    # Derivate festures (realtive humidity, wind speed, wind direction)
    # Transform kelvin data to celsius
    df = df.with_columns([
        np.sqrt(pl.col("windSpeedEast") ** 2 + pl.col("windSpeedNorth") ** 2).alias("windSpeed"),
        ((180 + np.degrees(np.arctan2(pl.col("windSpeedEast"), pl.col("windSpeedNorth")))) % 360).alias("windDirection"),
        (pl.col("dewAirTemperature") - 273.15).alias("dewAirTemperature"),
        (pl.col("airTemperature") - 273.15).alias("airTemperature")
    ])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = df.with_columns(
            pl.when(pl.col("airTemperature").is_null() | pl.col("dewAirTemperature").is_null())
            .then(None)
            .otherwise(100* np.exp((17.625 *  pl.col("dewAirTemperature")) / (243.04 +  pl.col("dewAirTemperature")))/
                    np.exp((17.625 * pl.col("airTemperature")) / (243.04 + pl.col("airTemperature")))
                    ).alias("relativeHumidity")
        )
        
    # Accummulative parameters: https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#ERA5:datadocumentation-Table3
    # Transform them to instantaneous
    acumm =  [ 'GHI','thermalRadiation']
    df = df.with_columns(
        pl.when(pl.col("time") == df.select(pl.col("time").min()).item())
        .then(0)
        .when(pl.col("time") == df.select(pl.col("time").max()).item())
        .then(0)
        .otherwise((pl.col("thermalRadiation").shift(-1) - pl.col("thermalRadiation")) / 3600)
        .alias("thermalRadiation_avg")
    )
    df = df.with_columns(
        pl.when(pl.col("time") == df.select(pl.col("time").min()).item())
        .then(0)
        .when(pl.col("time") == df.select(pl.col("time").max()).item())
        .then(0)
        .otherwise((pl.col("GHI").shift(-1) - pl.col("GHI")) / 3600)
        .alias("GHI_avg")
    )

    df = df.with_columns(
        pl.when(pl.col("GHI_avg") > 0)
        .then(pl.col("GHI_avg"))
        .otherwise(0)
        .alias("GHI_avg")
    )
    df = df.with_columns(
        pl.when(pl.col("thermalRadiation_avg") > 0)
        .then(pl.col("thermalRadiation_avg"))
        .otherwise(0)
        .alias("thermalRadiation_avg")
    )

    # Instantaneous parameters: https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#ERA5:datadocumentation-Table2
    # analysis = [x for x in era_5.values() if x not in acumm] + ["windSpeed"] + ["windDirection"] + ["relativeHumidity"]
    for var in ["windSpeed",  "dewAirTemperature", "airTemperature", "relativeHumidity"]:
        df = df.with_columns(
            pl.when(pl.col("time") < df.select(pl.col("time").max()).item())
            .then((pl.col(var).shift(-1) + pl.col(var)) / 2)
            .otherwise(pl.col(var))
            .alias(f"{var}_avg")
        )
    df = df.drop(["windSpeed",  "dewAirTemperature", "airTemperature", "relativeHumidity"])
    df = df.drop(acumm)
    rename_map = {col: col.replace("_avg", "") for col in df.columns if col.endswith("_avg")}
    df = df.rename(rename_map)

    # Loop through each group and apply the join_solar_data function
    result_list = []
    for _, group_df in df.group_by(["latitude","longitude"]):
        result = join_solar_features(group_df)
        result_list.append(result)

    return pl.concat(result_list)



def agg_by_postalcodes(df):
    lat1 = df.select(['latitude', 'longitude']).unique()["latitude"]
    lon1 = df.select(['latitude', 'longitude']).unique()["longitude"]
    features = df.drop(['time','latitude', 'longitude']).columns
    points = np.vstack((lat1, lon1)).T

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        
        gdf = gpd.read_file(postalcodes_path)
        gdf = gdf.to_crs(epsg=4326)
        gdf = gdf.dissolve(by='COD_POSTAL')
        gdf = gdf.reset_index()
        
        gdf['centroid'] = gdf.geometry.centroid
        lat2 = gdf['centroid'].y
        lon2 = gdf['centroid'].x
        grid_points = np.vstack((lat2, lon2)).T


    dfs =[]
    for group_name, group_df in df.group_by(["time"]):
        result_df = pl.DataFrame({
            'postal_code': gdf['COD_POSTAL'],
            'latitude': lat2,
            'longitude': lon2,
            'time': group_name * len(lon2)
        })
        try:
            for feature in features:
                values = group_df[feature]
                interpolated_values = griddata(points, values, grid_points, method='linear')
                nearest_values = griddata(points, values, grid_points, method='nearest')
                interpolated_values = np.where(np.isnan(interpolated_values), nearest_values, interpolated_values)
                        
                result_df.insert_column(-1,pl.Series(feature,interpolated_values))

            dfs.append(result_df) 
        except:
            group_coords = result_df.select(['latitude', 'longitude']).unique()
            df_coords = group_df.select(['latitude', 'longitude']).unique()

            # Convert to sets of tuples for comparison
            group_set = set(map(tuple, group_coords.to_numpy()))
            df_set = set(map(tuple, df_coords.to_numpy()))

            # Find missing values in df compared to group_df
            missing_in_df =  df_set -group_set
            print(f"The {group_name} with these {features} is missing {missing_in_df}")
    return pl.concat(dfs)
    
    
def transform_data(filename): 
    # Converting Byte data to DataFrame
    df = grib2df(filename)
    # Cleaning pipeline
    ## Removing missing values
    df = df.fill_nan(None)
    df = df.drop_nulls() 
    ## Rename features

    era_5 = {x['shortName']:x['era5_name'] for x in variables}
    df = df.rename(era_5)
    
    # Ingest geolocation
    df = agg_by_postalcodes(df)
    # Transformation pipeline
    ## Filter locations
    df = filter_locations(postalcodes_path,df)
    df = transform_features(df)
    
    pandas_dataframe = df.to_pandas()
    df_null = pandas_dataframe.where(pd.notnull(pandas_dataframe), None)
    print(df_null[df_null.isnull().any(axis=1)])
    print(df)
    return df
    
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
   
    
if __name__ == "__main__":
    directory = '/energycat/climateDT/11/*'
    files = glob.glob(directory)
    bucket = "climate-dt"
    s3 = setup_S3("lumi")
    for file in files:
        t = file.split("/")[-1]
        dt = datetime.strptime(t.split("_")[0], '%Y-%m-%dT%H:%M:%SZ')
        date = dt.strftime('%Y%m%d')
        month = dt.strftime('%m')
        year = dt.strftime('%Y')
        filename = f"{year}/{month}/{date}_{file.split('_', 1)[1]}"
        print(f"uploading {file}")
        try:
            s3.head_object(Bucket=bucket,Key=filename)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                s3.upload_file(file,bucket,filename)
                df = transform_data(file)
                upload_data(df,table_name="climatedt")
            else:
                raise


# if __name__ == "__main__":
#     directory = '/energycat/climateDT/07'
#     for file in os.listdir(directory):
        
#         filename = os.fsdecode(file)
#         path = f"{directory}/{filename}"
#         print(f"uploading {filename}")
        
#         df = transform_data(path)
#         upload_data(df,table_name="climatedt")
        
        


        
        
        
        

