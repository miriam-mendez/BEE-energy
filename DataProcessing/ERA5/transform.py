import numpy as np
import pygrib
import pandas as pd
import polars as pl
from datetime import datetime
import geopandas as gpd
from shapely.geometry import Point
import warnings
from scipy.interpolate import griddata
import calendar
import pvlib
from tqdm import tqdm

postalcodes_path = '/home/eouser/Desktop/SpanishPostalCodes/Catalonia/postal_codes.shp'
era_5 = {'10u': 'windSpeedEast',
        '10v': 'windSpeedNorth',
        '2d': 'dewAirTemperature',
        '2t': 'airTemperature',
        'lai_hv': 'highVegetationRatio',
        'lai_lv': 'lowVegetationRatio',
        'tp': 'totalPrecipitation',
        'ssrd': 'GHI',
        'fal': 'albedo',
        'stl4': 'soilTemperature',
        'swvl4': 'soilWaterRatio',
        'sp': 'surfacePressure'
    }

def grib2df(bytes_data,filename):
    messages = bytes_data.split(b'7777')[:-1]
    print(len(messages))
    slc = len(era_5) # number of features
    df_ = pl.DataFrame()
    for i in tqdm(range(0,len(messages),slc), total= (len(messages)//slc), desc="Convertirng grib files to df"):
        try:
            data = {
                'latitude': [],
                'longitude': [],
                'time': []
            }
            for message in messages[i:i+slc]:
                grb = pygrib.fromstring(message + b'7777') 
                # print(grb)
                # Replace 9999 with np.nan in data array
                data_array = np.where(grb.values.data == 9999, np.nan, grb.values.data)
                data.update({grb.shortName: data_array.flatten()})
               
            lats, lons = map(lambda x: np.round(x, 1), grb.latlons())
            if len(str(grb.validityTime)) < 4:
                time = datetime.strptime(f"{grb.validityDate} 0{grb.validityTime}", '%Y%m%d %H%M')
            else:
                time = datetime.strptime(f"{grb.validityDate} {grb.validityTime}", '%Y%m%d %H%M')
                
            # Flatten arrays and append data
            data['latitude'].extend(lats.flatten())
            data['longitude'].extend(lons.flatten())
            data['time'].extend([time] * len(data_array.flatten()))
            new_df = pl.DataFrame(data)
            # print(new_df)
            # print(f"columns new df: {new_df.columns}")
            # print(f"columns old df: {df_.columns}")
            # Convert data to Polars DataFrame and concatenate
            df_ = pl.concat([df_, new_df]) 
        except Exception as e:
            print(f"Error in message {i}")
            print(e)
    print(df_)
    return df_
# swvl4

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
        (pl.col("soilTemperature") - 273.15).alias("soilTemperature"),
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
    acumm =  ['totalPrecipitation', 'GHI']
    df = df.with_columns(
        pl.when(pl.col("time") == df.select(pl.col("time").min()).item())
        .then(0)
        .when(pl.col("time") == df.select(pl.col("time").max()).item())
        .then(0)
        .otherwise((pl.col("GHI").shift(-1) - pl.col("GHI")) / 3600)
        .alias("GHI_avg")
    )
    df = df.with_columns(
        pl.when(pl.col("time") == df.select(pl.col("time").min()).item())
        .then(0)
        .when(pl.col("time") == df.select(pl.col("time").max()).item())
        .then(pl.col("totalPrecipitation"))
        .otherwise((pl.col("totalPrecipitation").shift(-1) - pl.col("totalPrecipitation")) * 1000)
        .alias("totalPrecipitation_avg")
    )
    df = df.with_columns(
        pl.when(pl.col("GHI_avg") > 0)
        .then(pl.col("GHI_avg"))
        .otherwise(0)
        .alias("GHI_avg")
    )
    df = df.with_columns(
        pl.when(pl.col("totalPrecipitation_avg") > 0)
        .then(pl.col("totalPrecipitation_avg"))
        .otherwise("totalPrecipitation")
        .alias("totalPrecipitation_avg")
    )

    # Instantaneous parameters: https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#ERA5:datadocumentation-Table2
    # analysis = [x for x in era_5.values() if x not in acumm] + ["windSpeed"] + ["windDirection"] + ["relativeHumidity"]
    for var in ["windSpeed", "soilTemperature", "dewAirTemperature", "airTemperature", "relativeHumidity"]:
        df = df.with_columns(
            pl.when(pl.col("time") < df.select(pl.col("time").max()).item())
            .then((pl.col(var).shift(-1) + pl.col(var)) / 2)
            .otherwise(pl.col(var))
            .alias(f"{var}_avg")
        )
    df = df.drop(["windSpeed", "soilTemperature", "dewAirTemperature", "airTemperature", "relativeHumidity"])
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
    
    
def transform_data(bytes_data): 
    # Converting Byte data to DataFrame
    df = pl.DataFrame()
    for filename,data in bytes_data.items():
        if df.shape[0] == 0:
            df = grib2df(data,filename)
        else:
            df2 = grib2df(data,filename)
            df = df.join(df2, on=["latitude", "longitude", "time"], how="left")
    
    # Cleaning pipeline
    ## Removing missing values
    df = df.fill_nan(None)
    df = df.drop_nulls() 
    ## Rename features
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
    return df
    
    
    



