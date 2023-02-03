from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url) -> pd.DataFrame:
    """Read data from web into pandas"""

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color == "yellow":
        dtstr = "tpep"
    elif color == "green":
        dtstr = "lpep"

    df[f"{dtstr}_pickup_datetime"] = pd.to_datetime(df[f"{dtstr}_pickup_datetime"])
    df[f"{dtstr}_dropoff_datetime"] = pd.to_datetime(df[f"{dtstr}_dropoff_datetime"])
    # print(df.head(2))
    # print(f"columns: {df.dtypes}")
    print(f"Before no passengers: {len(df)}")

    df = df[df["passenger_count"] != 0]
    print(f"After no passengers: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DF out as local parquet"""

    path = Path(f"../data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True, retries=3)
def write_gsc(path: Path, color: str, dataset_file: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dezoomcamp1")
    gcs_block.upload_from_path(
        from_path=path, to_path=f"data/{color}/{dataset_file}.parquet"
    )
    return


@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """Main ETL function"""

    # color = "yellow"
    # year = 2021
    # month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df_raw = fetch(dataset_url)
    df_clean = clean(df_raw, color)
    path = write_local(df_clean, color, dataset_file)
    write_gsc(path, color, dataset_file)


@flow()
def etl_parent_flow(
    months: list[int] = [1], year: int = 2021, color: str = "green"
) -> None:
    for month in months:
        etl_web_to_gcs(color, year, month)


if __name__ == "__main__":
    etl_parent_flow()


# prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
# prefect deployment apply etl_parent_flow-deployment.yaml
