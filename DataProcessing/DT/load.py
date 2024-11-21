from sqlalchemy import create_engine, text
import yaml
import psycopg2
import polars as pl
from tqdm import tqdm

with open('/home/eouser/Desktop/DEDL/credentials.yaml', 'r') as f:
    c = yaml.safe_load(f)["postgres"]
    
conn = psycopg2.connect(f"dbname={c['db_name']} user={c['db_user']} password={c['db_password']} host={c['db_host']} port={c['db_port']} sslmode=require")
cursor = conn.cursor()


dtype_map = {
    'object': 'VARCHAR',
    'int64': 'INTEGER',
    'float64': 'FLOAT',
    'datetime64[ns]': 'TIMESTAMP',
    'datetime64[us]': 'TIMESTAMP'
}


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
