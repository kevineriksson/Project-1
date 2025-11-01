# --- IMPORTS ---
from datetime import datetime, timedelta
import os
import pandas as pd
import gzip
import io
import requests
import subprocess
import clickhouse_connect
import zipfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


# --- ENV VARIABLES ---
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse-server")
CH_USER = os.environ.get("CLICKHOUSE_USER", "dbt_user")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "dbt_password")

KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME")
KAGGLE_KEY = os.environ.get("KAGGLE_KEY")

# --- FILE PATHS ---
RAW_DIR = "/opt/airflow/dags/raw"
os.makedirs(RAW_DIR, exist_ok=True)
TMDB_CSV_LOCAL = "movies_tmdb.csv"

IMDB_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_CREW_URL = "https://datasets.imdbws.com/title.crew.tsv.gz"
IMDB_NAMES_URL = "https://datasets.imdbws.com/name.basics.tsv.gz"


# --- HELPERS ---
def ensure_raw_dir():
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR, exist_ok=True)


# --- TASK 5: LOAD INTO CLICKHOUSE (CHUNKED + QUALITY + IDEMPOTENT) ---
def load_clickhouse_bronze(**context):
    client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)
    client.command("SET max_insert_block_size = 500000")

    # ------------------- TMDb -------------------
    tmdb_file = os.path.join(RAW_DIR, TMDB_CSV_LOCAL)
    tmdb_df = pd.read_csv(tmdb_file)

    # Must have imdb_id column
    if "imdb_id" not in tmdb_df.columns:
        raise AirflowFailException("TMDb file missing 'imdb_id' column")

    # Keep only relevant fields for the analytical model
    expected_cols = [
        "imdb_id", "title", "release_date",
        "budget", "revenue", "vote_average",
        "vote_count", "runtime", "genres"
    ]
    tmdb_df = tmdb_df[[c for c in tmdb_df.columns if c in expected_cols]]

    # Drop rows missing both imdb_id and title
    tmdb_df = tmdb_df.dropna(subset=["imdb_id", "title"], how="all")

    # --- Data quality check: null imdb_id ---
    missing = tmdb_df["imdb_id"].isna().sum()
    if missing > 0:
        print(f"[QUALITY CHECK] Dropping {missing} TMDb rows with null imdb_id")
    tmdb_df = tmdb_df.dropna(subset=["imdb_id"])

    # Fill NaNs for text columns and cast to string
    for col in ["imdb_id", "title", "genres"]:
        if col in tmdb_df.columns:
            tmdb_df[col] = tmdb_df[col].fillna("").astype(str)

    # Convert release_date to proper date object for ClickHouse Date32
    if "release_date" in tmdb_df.columns:
        tmdb_df["release_date"] = pd.to_datetime(
            tmdb_df["release_date"], errors="coerce"
        ).dt.date

    # Drop rows with invalid or missing release_date
    tmdb_df = tmdb_df.dropna(subset=["release_date"])

    # --- Idempotent insert: clear table before load ---
    print("Clearing bronze.tmdb_raw for idempotent load")
    client.command("TRUNCATE TABLE IF EXISTS bronze.tmdb_raw")

    # Insert cleaned TMDb data into ClickHouse
    client.insert_df("bronze.tmdb_raw", tmdb_df)
    print(f"Inserted {len(tmdb_df)} TMDb rows into bronze.tmdb_raw")

    # ------------------- IMDb title.basics -------------------
    print("Clearing bronze.imdb_title_basics_raw for idempotent load")
    client.command("TRUNCATE TABLE bronze.imdb_title_basics_raw")

    basics_file = os.path.join(RAW_DIR, "title.basics.tsv")
    for i, chunk in enumerate(pd.read_csv(
        basics_file,
        sep="\t",
        na_values="\\N",
        usecols=["tconst", "primaryTitle", "startYear", "runtimeMinutes"],
        dtype=str,
        low_memory=False,
        chunksize=500000
    )):
        chunk["startYear"] = pd.to_numeric(chunk["startYear"], errors="coerce").fillna(-1).astype("int32")
        chunk["runtimeMinutes"] = pd.to_numeric(chunk["runtimeMinutes"], errors="coerce").fillna(-1).astype("int32")
        chunk["primaryTitle"] = chunk["primaryTitle"].fillna("").astype(str)
        chunk = chunk.dropna(subset=["tconst"])

        # Data-quality check: duplicates
        if chunk["tconst"].duplicated().any():
            raise AirflowFailException(f"Duplicate tconst found in chunk {i+1}")

        client.insert_df("bronze.imdb_title_basics_raw", chunk)
        print(f"Inserted title.basics chunk {i+1} ({len(chunk)} rows)")

    # ------------------- IMDb title.crew -------------------
    print("Clearing bronze.imdb_title_crew_raw for idempotent load")
    client.command("TRUNCATE TABLE bronze.imdb_title_crew_raw")

    crew_file = os.path.join(RAW_DIR, "title.crew.tsv")
    for i, chunk in enumerate(pd.read_csv(
        crew_file,
        sep="\t",
        na_values="\\N",
        usecols=["tconst", "directors"],
        dtype=str,
        low_memory=False,
        chunksize=500000
    )):
        chunk["directors"] = chunk["directors"].fillna("").astype(str)
        chunk = chunk.dropna(subset=["tconst"])
        client.insert_df("bronze.imdb_title_crew_raw", chunk)
        print(f"Inserted title.crew chunk {i+1} ({len(chunk)} rows)")

    # ------------------- IMDb name.basics -------------------
    print("Clearing bronze.imdb_name_basics_raw for idempotent load")
    client.command("TRUNCATE TABLE bronze.imdb_name_basics_raw")

    names_file = os.path.join(RAW_DIR, "name.basics.tsv")
    for i, chunk in enumerate(pd.read_csv(
        names_file,
        sep="\t",
        na_values="\\N",
        usecols=["nconst", "primaryName", "primaryProfession"],
        dtype=str,
        low_memory=False,
        chunksize=500000
    )):
        chunk["primaryName"] = chunk["primaryName"].fillna("").astype(str)
        chunk["primaryProfession"] = chunk["primaryProfession"].fillna("").astype(str)
        chunk = chunk.dropna(subset=["nconst"])
        client.insert_df("bronze.imdb_name_basics_raw", chunk)
        print(f"Inserted name.basics chunk {i+1} ({len(chunk)} rows)")

    print("All bronze tables successfully loaded.")


# --- TASK 6: RUN DBT TRANSFORMATIONS ---
def run_dbt_gold(**context):
    cmd = [
        "dbt", "run",
        "--project-dir", "/opt/airflow/project_root/dbt",
        "--profiles-dir", "/opt/airflow/project_root/dbt",
        "--select", "gold"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise AirflowFailException(f"dbt run failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout




# --- TASK 7: RUN DBT TESTS ---
def run_dbt_tests(**context):
    cmd = [
        "dbt", "test",
        "--project-dir", "/opt/dbt",
        "--profiles-dir", "/opt/dbt",
        "--select", "gold"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise AirflowFailException(f"dbt test failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout


# --- DAG CONFIG ---
default_args = {
    "owner": "data-eng-team14",
    "retries": 0,
    "depends_on_past": False,
}

with DAG(
    dag_id="gold_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    description="Ingest TMDb + IMDb → ClickHouse Bronze → dbt Gold",
) as dag:

    t_load = PythonOperator(task_id="load_clickhouse_bronze", python_callable=load_clickhouse_bronze)
    t_dbt = PythonOperator(task_id="run_dbt_gold", python_callable=run_dbt_gold)
    t_test = PythonOperator(task_id="run_dbt_tests", python_callable=run_dbt_tests)

    t_load >> t_dbt >> t_test
