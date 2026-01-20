from io import BytesIO
from pathlib import Path

from prefect import flow, task
import time

from config import BUCKET_BRONZE, BUCKET_SOURCES, get_minio_client

@task(name="upload_to_sources", retries=2)
def upload_csv_to_souces(file_path: str, object_name: str) -> str:
    """
    Upload local CSV file to MinIO sources bucket.

    Args:
        file_path: Path to local CSV file
        object_name: Name of object in MinIO

    Returns:
        Object name in MinIO
    """

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SOURCES):
        client.make_bucket(BUCKET_SOURCES)

    client.fput_object(BUCKET_SOURCES, object_name, file_path)
    print(f"Uploaded {object_name} to {BUCKET_SOURCES}")
    return object_name

@task(name="copy_to_bronze", retries=2)
def copy_to_bronze_layer(object_name: str, keep_history: bool = True) -> str:
    """
    Copy data from sources to bronze bucket (raw data lake layer) with versioning and backup.
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)

    # Lecture des données sources
    response = client.get_object(BUCKET_SOURCES, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    # --- Historisation ---
    if keep_history:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        history_name = f"history/{object_name.replace('.csv','')}_{timestamp}.csv"
        client.put_object(
            BUCKET_BRONZE,
            history_name,
            BytesIO(data),
            length=len(data)
        )
        print(f"Saved historical backup: {history_name}")

    # --- Copie principale dans Bronze ---
    client.put_object(
        BUCKET_BRONZE,
        object_name,
        BytesIO(data),
        length=len(data)
    )
    print(f"Copied {object_name} to {BUCKET_BRONZE}")

    # --- Backup final pour sécurité ---
    backup_name = f"backup/{object_name}"
    client.put_object(
        BUCKET_BRONZE,
        backup_name,
        BytesIO(data),
        length=len(data)
    )
    print(f"Duplicated backup: {backup_name}")

    return object_name

@flow(name="Bronze Ingestion Flow")
def bronze_ingestion_flow(data_dir: str = "./data/sources") -> dict:
    """
    Upload all CSV files in data_dir to sources and copy to bronze with history & backup.
    """
    client = get_minio_client()
    data_path = Path(data_dir)
    ingested_files = {}

    # Parcours dynamique de tous les fichiers CSV présents
    for file_path in data_path.glob("*.csv"):
        object_name = file_path.name
        # Upload dans sources
        uploaded = upload_csv_to_souces(str(file_path), object_name)
        # Copie dans Bronze avec versioning
        bronze_obj = copy_to_bronze_layer(uploaded)
        ingested_files[object_name.replace(".csv", "")] = bronze_obj

    return ingested_files

if __name__ == "__main__":
    result = bronze_ingestion_flow()
    print(f"Bronze ingestion complete: {result}")