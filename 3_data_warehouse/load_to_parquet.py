from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd
import os


@task(log_prints=True, retries=3)
def extract_from_ghub(file: str) -> Path:
    """download tripdata from github and convert to parquet"""

    df = pd.read_csv(
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{file}.csv.gz"
    )
    local_path = f"./data/{file}.parquet"
    df.to_parquet(local_path, compression="gzip")
    return local_path


@task(log_prints=True, retries=3)
def upload_data(local_path: str, file: str) -> None:
    """Uploads local data to GCP"""

    gcs_block = GcsBucket.load("dezoomcamp1")
    gcs_block.upload_from_path(from_path=local_path, to_path=f"fhv/{file}.parquet")
    return


@flow(log_prints=True)
def main() -> None:
    years = [2019, 2020, 2021]
    for year in years:
        for month in range(1, 13):
            if year == 2021 and month > 7:
                break
            file = f"fhv_tripdata_{year}-{month:02}"
            local_path = extract_from_ghub(file)
            upload_data(local_path, file)


if __name__ == "__main__":
    main()
