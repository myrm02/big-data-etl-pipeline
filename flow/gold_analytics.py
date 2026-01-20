import pandas as pd
from io import BytesIO
from prefect import task, flow
import json
from datetime import datetime
from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client

# =========================
# Helpers – Schema handling
# =========================

def ensure_columns(df: pd.DataFrame, defaults: dict) -> pd.DataFrame:
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df

def has_columns(*dfs: pd.DataFrame, cols: set[str]) -> bool:
    all_cols = set().union(*(df.columns for df in dfs))
    return cols.issubset(all_cols)

# =========================
# Quality & Metrics
# =========================

def compute_quality_metrics(fact: pd.DataFrame) -> dict:
    return {
        "row_count": int(len(fact)),
        "null_amount_pct": float(fact["amount"].isna().mean()),
        "negative_amount_pct": float((fact["amount"] < 0).mean()),
    }

def compute_extended_kpis(df: pd.DataFrame) -> pd.DataFrame:
    df_valid = df.dropna(subset=["amount"])
    ca_total = df_valid["amount"].sum()
    tx_count = len(df_valid)
    return pd.DataFrame({
        "total_ca": [ca_total],
        "total_transactions": [tx_count],
        "avg_panier": [df_valid["amount"].mean() if not df_valid.empty else None],
        "median_panier": [df_valid["amount"].median() if not df_valid.empty else None],
        "max_panier": [df_valid["amount"].max() if not df_valid.empty else None],
        "ca_per_transaction": [ca_total / max(tx_count, 1)],
    })

def compare_kpis_detailed(current: pd.DataFrame, previous: pd.DataFrame | None) -> dict:
    all_metrics = ["total_ca", "total_transactions", "avg_panier",
                   "median_panier", "max_panier", "ca_per_transaction"]
    deltas = {}
    pct_changes = {}
    for metric in all_metrics:
        cur = current[metric].iloc[0] if metric in current.columns else None
        prev = previous[metric].iloc[0] if previous is not None and metric in previous.columns else None
        if pd.notna(cur) and pd.notna(prev):
            deltas[metric] = float(cur - prev)
            pct_changes[metric] = float((cur - prev) / max(prev, 1))
        else:
            deltas[metric] = None
            pct_changes[metric] = None
    return {
        "has_previous": previous is not None,
        "delta": deltas,
        "pct_change": pct_changes
    }

def compute_ca_concentration(df: pd.DataFrame) -> float:
    if not {"client_id", "amount"}.issubset(df.columns):
        return 0.0
    ca_by_client = df.groupby("client_id")["amount"].sum().sort_values(ascending=False)
    if ca_by_client.empty:
        return 0.0
    top_20_pct = max(int(len(ca_by_client) * 0.2), 1)
    return float(ca_by_client.iloc[:top_20_pct].sum() / ca_by_client.sum())

# =========================
# Dimensions & Facts
# =========================

def build_dim_client(df_clients: pd.DataFrame) -> pd.DataFrame:
    for col in ["id", "name", "country", "age"]:
        if col not in df_clients.columns:
            df_clients[col] = pd.NA
    return df_clients[["id", "name", "country", "age"]].drop_duplicates(subset=["id"])

def build_dim_date(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if date_col not in df.columns:
        df[date_col] = pd.NaT
    dates = pd.to_datetime(df[date_col], errors="coerce").dropna().unique()
    df_date = pd.DataFrame({"date": dates})
    df_date["day"] = df_date["date"].dt.day
    df_date["week"] = df_date["date"].dt.isocalendar().week
    df_date["month"] = df_date["date"].dt.month
    df_date["year"] = df_date["date"].dt.year
    df_date["month_name"] = df_date["date"].dt.month_name()
    return df_date

def agg_ca(df: pd.DataFrame, period: str) -> pd.DataFrame:
    return df.groupby(period).agg(ca=("amount", "sum"), volume=("amount", "count")).reset_index()

def agg_ca_pays(df: pd.DataFrame) -> pd.DataFrame:
    if "country" not in df.columns:
        df["country"] = "UNKNOWN"
    return df.groupby("country").agg(ca=("amount", "sum")).reset_index()

# =========================
# MinIO IO
# =========================

def read_parquet_from_minio(client, bucket: str, object_name: str) -> pd.DataFrame:
    response = client.get_object(bucket, object_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    return df

# =========================
# GOLD TASK
# =========================

@task(name="silver_to_gold", retries=2)
def silver_to_gold() -> None:
    client = get_minio_client()
    run_ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    # Read Silver
    clients = read_parquet_from_minio(client, BUCKET_SILVER, "clients.parquet")
    achats = read_parquet_from_minio(client, BUCKET_SILVER, "achats.parquet")

    # Normalize schema
    clients = clients.rename(columns={"id_client": "id", "nom": "name", "pays": "country"})
    achats = achats.rename(columns={"id_achat": "id", "id_client": "client_id",
                                    "montant": "amount", "date_achat": "purchase_date"})

    # Enrich achats with country
    if has_columns(achats, clients, cols={"client_id", "id"}):
        achats = achats.merge(clients[["id", "country"]].rename(columns={"id": "client_id"}),
                              on="client_id", how="left")

    # Fact table schema-on-read
    fact_achat = ensure_columns(achats, {"purchase_date": pd.NaT, "amount": pd.NA, "country": "UNKNOWN"})
    fact_achat["purchase_date"] = pd.to_datetime(fact_achat["purchase_date"], errors="coerce")
    fact_achat["day"] = fact_achat["purchase_date"].dt.date
    fact_achat["month"] = fact_achat["purchase_date"].dt.to_period("M").astype(str)

    # Dimensions
    dim_client = build_dim_client(clients)
    dim_date = build_dim_date(fact_achat, "purchase_date")

    # Aggregations & KPIs
    agg_day = agg_ca(fact_achat, "day")
    agg_month = agg_ca(fact_achat, "month")
    agg_country = agg_ca_pays(fact_achat)
    kpis = compute_extended_kpis(fact_achat)
    quality = compute_quality_metrics(fact_achat)
    ca_concentration = compute_ca_concentration(fact_achat)

    # Compare with previous
    try:
        prev_kpis = read_parquet_from_minio(client, BUCKET_GOLD, "current/kpi_global.parquet")
    except Exception:
        prev_kpis = None

    kpi_comparison = compare_kpis_detailed(kpis, prev_kpis)

    # Write history
    base_path = f"history/run_ts={run_ts}"
    tables = {
        "dim_client": dim_client,
        "dim_date": dim_date,
        "fact_achat": fact_achat,
        "agg_ca_jour": agg_day,
        "agg_ca_mois": agg_month,
        "agg_ca_pays": agg_country,
        "kpi_global": kpis,
        "kpi_comparison": pd.DataFrame([
            {"metric": metric, "delta": delta, "pct_change": kpi_comparison["pct_change"][metric]}
            for metric, delta in kpi_comparison["delta"].items()
        ]),
    }

    for name, df in tables.items():
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        client.put_object(BUCKET_GOLD, f"{base_path}/{name}.parquet", buffer, buffer.getbuffer().nbytes)

    metadata = {
        "run_ts": run_ts,
        "quality": quality,
        "kpis": kpis.to_dict(orient="records")[0],
        "kpi_comparison": kpi_comparison,
        "ca_concentration_top_20_pct": ca_concentration,
    }

    client.put_object(BUCKET_GOLD, f"{base_path}/metadata.json",
                      BytesIO(json.dumps(metadata, indent=2).encode()),
                      len(json.dumps(metadata).encode()))

    # Always promote to current
    for name, df in tables.items():
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        client.put_object(BUCKET_GOLD, f"current/{name}.parquet", buffer, buffer.getbuffer().nbytes)

# =========================
# Flow
# =========================

@flow(name="Gold Analytics Flow v4")
def gold_ingestion_flow() -> None:
    silver_to_gold()

# =========================
# Main
# =========================

if __name__ == "__main__":
    gold_ingestion_flow()
    print("Gold ingestion complete")