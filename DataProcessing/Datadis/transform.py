import pandas as pd
import io
import polars as pl
from datetime import timedelta
from tqdm import tqdm

def transform_data(bytes_data):
    sector_dfs = {}
    filenames = []
    for filename, binary_content in tqdm(bytes_data.items(), desc="Transforming data"):
        file_like = io.BytesIO(binary_content)
        df = pl.read_csv(file_like, infer_schema_length=0)
        if df.is_empty():
            filenames.append(filename)
            continue
        if "0" in df.columns:
            filenames.append(filename)
            continue
        df = df.with_columns([pl.col(col).cast(pl.Float64) for col in df.columns[-25:]])
        df = df.drop(['','community', 'province', 'municipality', 'sumEnergy'])

        # Melt the dataframe
        df = df.melt(
            id_vars=['dataDay', 'dataMonth', 'dataYear', 'postalCode', 'fare',
                    'timeDiscrimination', 'measurePointType', 'sumContracts', 
                    'tension', 'economicSector', 'distributor'],
            value_vars=[f"mi{i}" for i in range(1, 25)],
            variable_name="hour_datadis",
            value_name="Consumption"
        )

        # Extract hour and pad with zero
        df = df.with_columns([
            pl.col("sumContracts").cast(pl.Float64),
            pl.col("hour_datadis").str.extract(r"(\d+)", 1).str.zfill(2).alias("hour")
        ])

        df = df.with_columns(
            (pl.concat_str([
                pl.col("dataYear"), pl.lit("-"),
                pl.col("dataMonth").str.zfill(2), pl.lit("-"),
                pl.col("dataDay").str.zfill(2)
            ]).str.strptime(pl.Date, "%Y-%m-%d")).alias("date")
        )

        # Mask where hour is '24' and adjust the date
        mask = pl.col("hour") == '24'
        df = df.with_columns([
            pl.when(mask).then(pl.lit("00")).otherwise(pl.col("hour")).alias("hour_correct"),
            pl.when(mask).then(pl.col("date") + pl.duration(days=1)).otherwise(pl.col("date")).alias("datetime")
        ])


        try: 
        # Create the 'time' column
            df = df.with_columns(
                (pl.concat_str([
                    pl.col("datetime").dt.strftime('%Y-%m-%d'), pl.lit(" "),
                    pl.col("hour_correct"), pl.lit(":00")
                ]).str.to_datetime().dt.replace_time_zone("Europe/Amsterdam",ambiguous='latest').alias("time"))
            )
        except:
            if df.filter(pl.col('dataMonth') == '3').height > 0:
            # Drop rows where 'hour_correct' is '03' and 'dataMonth' is '3'
                df = df.filter(~(pl.col('hour_correct') == '03'))
            
            # # Replace 'hour_correct' '02' with '03' where 'dataMonth' is '3'
            df = df.with_columns(
            hour_correct = pl.when(pl.col('hour_correct') == '02')
            .then(pl.lit('03'))  # Replace '02' with '03'
            .otherwise(pl.col('hour_correct'))) 

            df = df.with_columns(
                (pl.concat_str([
                    pl.col("datetime").dt.strftime('%Y-%m-%d'), pl.lit(" "),
                    pl.col("hour_correct"), pl.lit(":00")
                ]).str.to_datetime().dt.replace_time_zone("Europe/Amsterdam",ambiguous='latest').alias("time"))
            )


        # Drop unnecessary columns
        df = df.drop([
            'dataDay', 'dataMonth', 'dataYear', 'fare', 'timeDiscrimination', 
            'measurePointType', 'tension', 'distributor', 'hour', 'hour_correct','datetime','date', 'hour_datadis'
        ])
        # Group by postalCode, time, and economicSector, and sum the values
        df_grouped = df.group_by(['postalCode', 'time', 'economicSector']).agg([
            pl.sum('sumContracts'),
            pl.sum('Consumption')
        ])

        for sector, df_group in df_grouped.group_by('economicSector'):
            df_group = df_group.drop(['economicSector'])
            if sector[0] in sector_dfs:
                sector_dfs[sector[0]] = pl.concat([sector_dfs[sector[0]], df_group])
            else:
                sector_dfs[sector[0]] = df_group
    file = open ('empty_postalCodes.txt', 'a')    
    file.write(f"{filenames}\n")  
    file.close()  
    return sector_dfs