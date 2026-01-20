import pandas as pd
from io import BytesIO
from prefect import task, flow
import time
from bronze_ingestion import bronze_ingestion_flow
from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client

def detect_id_column(columns: list[str]) -> str | None:
    """
    Detect an ID column using safe heuristics.
    """
    for col in columns:
        if col == "id":
            return col
        if col.startswith("id_"):
            return col
        if col.endswith("_id"):
            return col
    return None

def clean_dataframe(df: pd.DataFrame, dtypes: dict | None = None) -> pd.DataFrame:
    """
    Apply Silver layer transformations:
    - drop rows fully empty
    - drop duplicates
    - cast types
    - normalize dates
    - handle aberrant values
    """
    # Drop fully empty rows
    df = df.dropna(how="all")

    # Strip column names and lowercase
    df.columns = [c.lower().strip() for c in df.columns]

    # Detect ID column and rename
    id_col = detect_id_column(df.columns)
    if not id_col:
        raise ValueError(f"No ID column detected: {df.columns.tolist()}")
    df = df.rename(columns={id_col: "id"})

    # Deduplicate
    df = df.drop_duplicates(subset=["id"])

    # Type casting
    if dtypes:
        for col, dtype in dtypes.items():
            if col in df.columns:
                try:
                    # For datetime columns
                    if "date" in col.lower() and dtype in ["datetime64[ns]", "datetime"]:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    else:
                        df[col] = df[col].astype(dtype)
                except Exception as e:
                    print(f"Warning: Failed to cast {col} to {dtype}: {e}")

    # Handle aberrant values (simple example: numeric negative -> NaN)
    for col, dtype in dtypes.items() if dtypes else []:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            df.loc[df[col] < 0, col] = pd.NA

    # Optional: Fill missing numeric values with median
    for col in df.select_dtypes(include=["number"]).columns:
        df[col] = df[col].fillna(df[col].median())

    # Optional: Fill missing categorical values with placeholder
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("UNKNOWN")

    return df

@task(name="bronze_to_silver", retries=2)
def bronze_to_silver(object_name: str, dtypes: dict | None = None) -> str:
    """
    Transform CSV from Bronze to Silver with:
    - cleaning & type normalization
    - history versioning
    - backup duplication
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    # 1️⃣ Lecture du CSV depuis Bronze
    response = client.get_object(BUCKET_BRONZE, object_name)
    df = pd.read_csv(response)
    response.close()
    response.release_conn()

    # 2️⃣ Nettoyage et normalisation
    df = clean_dataframe(df, dtypes=dtypes)

    # 3️⃣ Sauvegarde temporaire en mémoire
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # 4️⃣ Version horodatée pour historique
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    history_name = f"history/{object_name.replace('.csv','')}_{timestamp}.parquet"
    client.put_object(
        BUCKET_SILVER,
        history_name,
        BytesIO(buffer.getbuffer()),
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    print(f"Saved historical Silver version: {history_name}")

    # 5️⃣ Duplication pour backup
    backup_name = f"backup/{object_name.replace('.csv','')}.parquet"
    client.put_object(
        BUCKET_SILVER,
        backup_name,
        BytesIO(buffer.getbuffer()),
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    print(f"Backup duplicated: {backup_name}")

    # 6️⃣ Écrasement du fichier principal seulement si le résultat est valide
    # Simple exemple de validation : colonne ID présente et pas de valeurs critiques nulles
    if "id" in df.columns and not df["id"].isna().all():
        silver_object_name = object_name.replace(".csv", ".parquet")
        client.put_object(
            BUCKET_SILVER,
            silver_object_name,
            BytesIO(buffer.getbuffer()),
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        print(f"Updated Silver main file: {silver_object_name}")
    else:
        print(f"Validation failed. Silver main file not updated for {object_name}")
        silver_object_name = history_name  # on retourne la dernière version valide

    return silver_object_name

@flow(name="Silver Transformation Flow")
def silver_transform_flow(bronze_objects: dict) -> dict:
    silver_files = {}

    for table, obj_name in bronze_objects.items():
        if table == "clients":
            silver_files[table] = bronze_to_silver(
                obj_name,
                dtypes={"id": "int64", "age": "int64", "birth_date": "datetime64[ns]"}
            )
        elif table == "achats":
            silver_files[table] = bronze_to_silver(
                obj_name,
                dtypes={"id": "int64", "amount": "float64", "purchase_date": "datetime64[ns]"}
            )
        else:
            # Traitement générique pour nouvelles tables
            silver_files[table] = bronze_to_silver(obj_name)

    return silver_files

if __name__ == "__main__":
    result = silver_transform_flow(bronze_ingestion_flow())
    print(f"Silver transformation complete: {result}")