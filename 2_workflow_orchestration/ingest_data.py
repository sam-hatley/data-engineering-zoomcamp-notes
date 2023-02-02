import pandas as pd
from sqlalchemy import create_engine
import argparse
import os
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3)
def extract_data(url):
    csv_name = "output.csv"

    # Download CSV
    os.system(f"wget {url} -O {csv_name}.gz && gzip -d {csv_name}")

    df = pd.read_csv(f"{csv_name}")
    return df


@task(log_prints=True)
def transform_data(df):

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    print(f'pre: missing passenger count: {df["passenger_count"].isin([0]).sum()}')
    df = df[df["passenger_count"] != 0]
    print(f'post: missing passenger count: {df["passenger_count"].isin([0]).sum()}')
    return df


@task(log_prints=True)
def ingest_data(table_name, df):
    connection_block = SqlAlchemyConnector.load("postgres")

    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=f"{table_name}", con=engine, if_exists="replace")
        df.to_sql(
            name=f"{table_name}",
            con=engine,
            if_exists="append",
            chunksize=10000,
            method="multi",
        )


@flow(name="Ingest Flow")
def main():
    # Define params to be passed to function through command-line args
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    table_name = "yellow_taxi_data"
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    raw_data = extract_data(url)
    clean_data = transform_data(raw_data)
    ingest_data(table_name, clean_data)


if __name__ == "__main__":
    main()
