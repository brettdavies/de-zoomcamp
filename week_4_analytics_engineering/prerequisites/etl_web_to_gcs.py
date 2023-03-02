from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task()
def fetch(dataset_url: str, color: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    if color=="green":
        df = pd.read_csv(dataset_url).astype({
            "store_and_fwd_flag": "string"
        })
    elif color=="yellow":
        df = pd.read_csv(dataset_url).astype({
            "store_and_fwd_flag": "string"
        })
    elif color=="fhv":
        df = pd.read_csv(dataset_url).astype({
            "PUlocationID": "float64",
            "DOlocationID": "float64"
        })
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color=="yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    elif color=="green":
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    elif color=="fhv":
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

    else: True
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow(log_prints=True)
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"

    print(f"url: {dataset_url}")

    df = fetch(dataset_url, color)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(color: str, year: int):
    for month in range(12):
        etl_web_to_gcs(color, year, month+1)

if __name__ == "__main__":
    # etl_parent_flow("green", 2019)
    etl_parent_flow("green", 2020)
    # etl_parent_flow("yellow", 2019)
    # etl_parent_flow("yellow", 2020)
    # etl_parent_flow("fhv", 2019)
