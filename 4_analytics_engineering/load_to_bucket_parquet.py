from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd
from yaml import safe_load
import pyarrow
import os.path


@task(log_prints=True, retries=3)
def extract_from_ghub(color: str, file: str) -> pd.DataFrame:
    """download tripdata from github and convert to parquet"""
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file}.csv.gz"
    local_path = f"/tmp/{file}.parquet"

    try:
        df = pd.read_parquet(local_path)
    except FileNotFoundError:
        print(f"Local file {local_path} not found. Downloading from GitHub.")
        df = pd.read_csv(url, engine="pyarrow")

    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str, file: str) -> Path:
    """Fix dtype issues and save as parquet"""

    with open("./dtypes.yaml", "rb") as f:
        schema = safe_load(f)[color]

    df = df.astype(schema)

    local_path = f"/tmp/{file}.parquet"
    df.to_parquet(local_path, compression="gzip", engine="pyarrow")
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
    colors = ["yellow", "green"]
    years = [2019, 2020, 2021]
    months = [month for month in range(1, 13)]

    main(colors, years, months)
