import psycopg2
import polars as pl
import pandas as pd
from dask_gateway.auth import GatewayAuth
from dedllogin import DEDL_auth
from sqlalchemy import create_engine

class DESPAuth(GatewayAuth):
    def __init__(self):
        self.auth_handler = DEDL_auth('miriam.mendez.serrano@estudiantat.upc.edu', 'D2st3n1t34n21rth$')
        self.access_token = self.auth_handler.get_token()

    def pre_request(self, _):
        headers = {"Authorization": "Bearer " + self.access_token}
        return headers, None

# Connect to PostgreSQL and fetch data
def fetch_data_from_db(query, db_config, dtypes=None):
    conn = psycopg2.connect(**db_config)
    try:
        # Execute SQL query
        # data = pl.read_database(query, conn)

        # Fetch data with pandas to ensure schema consistency
        df = pd.read_sql_query(query, conn)

        if dtypes is not None:
            # Ensure consistent data types in Pandas DataFrame
            df = df.astype(dtypes)

        # Convert to Polars DataFrame
        data = pl.from_pandas(df)
    finally:
        conn.close()
    return data


def push_data_to_db(pandas_df, db_config, table_name):
    # Create SQLAlchemy engine
    try:
        engine = create_engine(
            f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["dbname"]}'
        )

        # Upload the pandas DataFrame to PostgreSQL using the SQLAlchemy engine
        pandas_df.to_sql(table_name, engine, if_exists='replace', index=False)
        return "Success!"
    except Exception as e:
        return f"Error: {e}"