import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    # Download CSV
    os.system(f"wget {url} -O {csv_name}.gz && gzip -d {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(f'{csv_name}', iterator=True, chunksize=100_000)
    df = next(df_iter)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    # Create table
    df.head(n=0).to_sql(name=f'{table_name}', con=engine, if_exists='replace')

    # Import the first 100,000 rows
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    print("inserted rows 0 - 100000")

    # Import the remaining rows
    n = 100_000
    while True:
        df = next(df_iter)
        
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

        print(f'inserted rows {n} - {n + 100_000}')
        n += 100_000

if __name__ == '__main__':
    # Define params to be passed to function through command-line args
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)