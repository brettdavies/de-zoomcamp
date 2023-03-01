# For loading FHV NY Taxi Data
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    return Path(f"{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"row count: {df['pickup_datetime'].count()}")
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("gcp-data")

    print(df.head(2))
    print(f"columns: {df.dtypes}")

    df.to_gbq(
        destination_table=f"nytaxi.{color}_trips_pyload",
        project_id="nytaxi-378623",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(color: str, year: int, month: int) -> None:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = path
    df = transform(path)
    write_bq(df, color)


@flow()
def etl_parent_flow(
        color: str, year: int, months: list[int]
):
    for month in months:
        etl_gcs_to_bq(color, year, month)

if __name__ == "__main__":
    color = "fhv"
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    etl_parent_flow(color, year, months)
