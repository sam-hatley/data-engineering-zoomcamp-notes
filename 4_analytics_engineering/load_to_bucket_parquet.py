from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd
import os.path

# Example URIs
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
# 2019-07 through 11 have dtype issues


@task(log_prints=True, retries=3)
def extract_from_ghub(color: str, file: str) -> pd.DataFrame:
    """download tripdata from github and convert to parquet"""

    df = pd.read_csv(
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file}.csv.gz"
    )
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str, file: str) -> Path:
    """Fix dtype issues and save as parquet"""

    if color == "yellow":
        dtstr = "tpep"
    elif color == "green":
        dtstr = "lpep"

    df[f"{dtstr}_pickup_datetime"] = pd.to_datetime(df[f"{dtstr}_pickup_datetime"])
    df[f"{dtstr}_dropoff_datetime"] = pd.to_datetime(df[f"{dtstr}_dropoff_datetime"])

    df = df.astype(
        {
            "VendorID": "Int64",
            f"{dtstr}_pickup_datetime": "datetime64",
            f"{dtstr}_dropoff_datetime": "datetime64",
            "passenger_count": "Int64",
            "trip_distance": "float64",
            "RatecodeID": "Int64",
            "store_and_fwd_flag": "object",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "payment_type": "Int64",
            "fare_amount": "float64",
            "extra": "float64",
            "mta_tax": "float64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "congestion_surcharge": "float64",
        }
    )

    local_path = f"/tmp/{file}.parquet"
    df.to_parquet(local_path, compression="gzip")
    return local_path


@task(log_prints=True, retries=3)
def upload_data(local_path: str, file: str, color: str) -> None:
    """Uploads local data to GCP"""

    gcs_block = GcsBucket.load("dezoomcamp1")
    gcs_block.upload_from_path(from_path=local_path, to_path=f"{color}/{file}.parquet")
    return


@flow(log_prints=True)
def main(colors: list, years: list, months: list) -> None:
    for color in colors:
        for year in years:
            if year == 2021 and months == [month for month in range(1, 13)]:
                months = [month for month in range(1, 8)]
            for month in months:
                file = f"{color}_tripdata_{year}-{month:02}"
                print(f"Processing {file}")
                df = extract_from_ghub(color, file)
                local_path = clean(df, color, file)
                upload_data(local_path, file, color)


if __name__ == "__main__":
    colors = ["green"]
    years = [2019, 2020, 2021]
    months = [month for month in range(1, 13)]

    main(colors, years, months)
