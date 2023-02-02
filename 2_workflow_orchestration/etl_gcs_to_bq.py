from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""

    # Provides for sep. between data lake and sql server
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dezoomcamp1")
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path=f"/home/sam/git/data-engineering-zoomcamp-notes/2_workflow_orchestration/",
    )
    return Path(f"./{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(
        f"pre: missing passenger count: {df['passenger_count'].isna().sum()}. Row count {len(df)}."
    )
    # df["passenger_count"].fillna(0, inplace=True) # Instructor's attempt
    df = df[df["passenger_count"].notna()]
    print(
        f"post: missing passenger count: {df['passenger_count'].isna().sum()}. Row count {len(df)}."
    )
    return df


@task(log_prints=True, retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """Write DF to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("dezoomcamp")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dezoomcamp-1",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(color: str = "yellow", year: int = 2021, month: int = 1) -> None:
    """Main ETL flow to load data into BigQuery"""

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
