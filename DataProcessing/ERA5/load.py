from sqlalchemy import create_engine, text
import yaml
import psycopg2
import polars as pl
from tqdm import tqdm

with open('/home/eouser/Desktop/DEDL/credentials.yaml', 'r') as f:
    c = yaml.safe_load(f)["postgres"]
    
engine = create_engine(f'postgresql://{c["db_user"]}:{c["db_password"]}@{c["db_host"]}:{c["db_port"]}/{c["db_name"]}')
conn = psycopg2.connect(f"dbname={c['db_name']} user={c['db_user']} password={c['db_password']} host={c['db_host']} port={c['db_port']} sslmode=require")
cursor = conn.cursor()


dtype_map = {
    'object': 'VARCHAR',
    'int64': 'INTEGER',
    'float64': 'FLOAT',
    'datetime64[ns]': 'TIMESTAMP',
    'datetime64[us]': 'TIMESTAMP'
}


# def check_table_exists(engine, table_name):
#     query = text(f"""
#         SELECT COUNT(*)
#         FROM information_schema.tables
#         WHERE table_schema = 'public' AND table_name = '{table_name}'
#     """)
#     with engine.connect() as conn:
#         result = conn.execute(query).fetchone()
#         return result[0] > 0

def check_table_exists(table_name):
    query = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = '{table_name}'
    """
    cursor.execute(query)
    return cursor.fetchone()[0] 
    

def check_data_exists(engine,table_name,start_date, end_date):
    query = text(f"""
        SELECT COUNT(*)
        FROM "{table_name}"
        WHERE time BETWEEN '{start_date}' AND '{end_date}'
    """)
    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
        return result
    
def upload_data(df, table_name):
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    print(len(df))
            
    if not check_table_exists(table_name):
        columns = []
        for col_name, dtype in df.dtypes.items():
            pg_type = dtype_map.get(str(dtype), 'VARCHAR')
            columns.append(f"{col_name} {pg_type}")
        create_table_query = f"""
        CREATE TABLE {table_name} (
            {', '.join(columns)},
            PRIMARY KEY (time, postal_code)
        );
        CREATE INDEX ON {table_name} (time, postal_code);
        SELECT create_hypertable('{table_name}','time');
        """
        cursor.execute(create_table_query)
        conn.commit()

    insert_query = f"""
    INSERT INTO {table_name} ({','.join(df.columns)})
    VALUES ({','.join(['%s'] * len(df.columns))});
    """

    for x in tqdm(df.to_numpy(), desc=f"Inserting {table_name} rows", unit="rows"):
        cursor.execute(insert_query,tuple(x))
    conn.commit()

def upload_data2(df, table_name):
    start_date = df.select("time").to_series().unique().min()
    end_date = df.select("time").to_series().unique().max()
    
    if check_table_exists(engine, table_name):
          
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        
        if not check_data_exists(engine,table_name,start_date_str,end_date_str):
            df.to_pandas().to_sql(table_name,engine,chunksize=100000, if_exists="append",index=False, method="multi")
            print(f"Monthly data for {start_date.strftime('%B, %Y')} appended successfully.")
        else:
            print(f"Data for {start_date.strftime('%B, %Y')} already exists in the table.")
    
    else:
        df.to_pandas().to_sql(table_name,engine,chunksize=100000, if_exists="replace",index=False, method="multi")
        print(f"Monthly data for {start_date.strftime('%B, %Y')} uploaded successfully.")
