import psycopg2
import yaml
from tqdm import tqdm
import polars as pl
import pandas as pd
with open('/home/eouser/Desktop/DEDL/credentials.yaml', 'r') as f:
    c = yaml.safe_load(f)["postgres"]
    
conn = psycopg2.connect(f"dbname={c['db_name']} user={c['db_user']} password={c['db_password']} host={c['db_host']} port={c['db_port']} sslmode=require")
cursor = conn.cursor()



def check_table_exists(table_name):
    query = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = '{table_name}'
    """
    cursor.execute(query)
    return cursor.fetchone()[0] 
    

def upload_data(df, table_name):
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()

            
    if not check_table_exists(table_name):
        create_table_query = f"""
        CREATE TABLE {table_name} (
            postalCode VARCHAR,
            time TIMESTAMPTZ,
            sumContracts FLOAT,
            Consumption FLOAT,
            PRIMARY KEY (postalCode, time)
        );
        CREATE INDEX ON {table_name} (postalCode, time);
        SELECT create_hypertable('{table_name}','time');
        """
        cursor.execute(create_table_query)
        conn.commit()

    insert_query = f"""
    INSERT INTO {table_name} (postalCode, time, sumContracts, Consumption)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (postalCode, time) DO NOTHING;
    """

    for x in tqdm(df.to_numpy(), desc=f"Inserting {table_name} rows", unit="rows"):
        cursor.execute(insert_query,tuple(x))
    conn.commit()

