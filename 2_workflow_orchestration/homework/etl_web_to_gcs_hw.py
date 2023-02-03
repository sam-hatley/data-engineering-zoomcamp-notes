from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.blocks.notifications import SlackWebhook


@task(retries=3)
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

    slack_block = SlackWebhook.load("webhook")

    slack_block.notify(f"Before no passengers: {len(df)}")
    df = df[df["passenger_count"] != 0]
    slack_block.notify(f"After no passengers: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DF out as local parquet"""

    path = Path(
        f"/home/sam/git/data-engineering-zoomcamp-notes/2_workflow_orchestration/data/{dataset_file}.parquet"
    )
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
def etl_web_to_gcs() -> None:
    """Main ETL function"""

    color = "green"
    year = 2020
    month = 11
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    print(dataset_url)
    df_raw = fetch(dataset_url)
    df_clean = clean(df_raw, color)
    # path = write_local(df_clean, color, dataset_file)
    # write_gsc(path, color, dataset_file)


if __name__ == "__main__":
    etl_web_to_gcs()
