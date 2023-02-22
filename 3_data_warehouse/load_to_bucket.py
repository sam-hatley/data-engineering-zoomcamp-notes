from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os


@task(log_prints=True, retries=3)
def extract_from_ghub(file: str) -> Path:
    """download tripdata from github"""

    os.system(
        f"wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{file} -O /tmp/{file}"
    )
    local_path = f"/tmp/{file}"
    return local_path


@task(log_prints=True, retries=3)
def upload_data(local_path: str, file: str) -> None:
    """Uploads local data to GCP"""

    gcs_block = GcsBucket.load("dezoomcamp1")
    gcs_block.upload_from_path(from_path=local_path, to_path=f"fhv/{file}")
    return


@flow(log_prints=True)
def main() -> None:
    years = [2019, 2020, 2021]
    for year in years:
        for month in range(1, 13):
            if year == 2021 and month > 7:
                break
            file = f"fhv_tripdata_{year}-{month:02}.csv.gz"
            local_path = extract_from_ghub(file)
            upload_data(local_path, file)


if __name__ == "__main__":
    main()
