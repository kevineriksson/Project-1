# --- IMPORTS ---
from datetime import datetime, timedelta
import os
import pandas as pd
import gzip
import io
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator # NEW: For dbt execution
from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook # NEW: For ClickHouse connection
from airflow.exceptions import AirflowFailException

# --- ENV VARIABLES (Not used for connection, kept for path/file config) ---
# Removed CH_HOST, CH_USER, CH_PASS as we will use the Airflow Connection ID
# CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse-server") # Not used
# CH_USER = os.environ.get("CLICKHOUSE_USER", "dbt_user") # Not used
# CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "dbt_password") # Not used

# --- FILE PATHS ---
RAW_DIR = "/opt/airflow/dags/raw"
os.makedirs(RAW_DIR, exist_ok=True)

TMDB_SOURCE = "movies_tmdb.csv"
IMDB_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_CREW_URL = "https://datasets.imdbws.com/title.crew.tsv.gz"

# --- TASK 1: VERIFY OR DOWNLOAD TMDB ---
def download_tmdb(**context):
    tmdb_path = os.path.join(RAW_DIR, TMDB_SOURCE)
    # NOTE: Your DAG logic requires the tmdb file to already be in ./airflow/dags/raw
    if not os.path.exists(tmdb_path):
        raise AirflowFailException(f"TMDb source file not found at {tmdb_path}. Please place it here before run.")
    return tmdb_path

# --- HELPER: DOWNLOAD AND UNPACK GZ FILES ---
def _download_gz_tsv(url, out_name):
    resp = requests.get(url, timeout=60)
    if resp.status_code != 200:
        raise AirflowFailException(f"Failed to download {url}")
    with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
        data = gz.read().decode("utf-8", errors="replace")
    out_path = os.path.join(RAW_DIR, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(data)
    return out_path

# --- TASK 2: DOWNLOAD IMDb BASICS ---
def download_imdb_basics(**context):
    return _download_gz_tsv(IMDB_BASICS_URL, "title.basics.tsv")

# --- TASK 3: DOWNLOAD IMDb CREW ---
def download_imdb_crew(**context):
    return _download_gz_tsv(IMDB_CREW_URL, "title.crew.tsv")

# --- TASK 4: LOAD ALL RAW FILES TO CLICKHOUSE BRONZE (Connection Fix Implemented) ---
def load_clickhouse_bronze(**context):
    # FIX: Use Airflow's ClickHouseHook which securely uses the connection defined in docker-compose
    hook = ClickHouseHook(clickhouse_conn_id="CLICKHOUSE_DEFAULT") 
    client = hook.get_client()

    # Create the bronze schema if it doesn't exist (assuming CH connection is fine)
    client.command("CREATE DATABASE IF NOT EXISTS bronze")

    # TMDb
    tmdb_file = os.path.join(RAW_DIR, TMDB_SOURCE)
    tmdb_df = pd.read_csv(tmdb_file)
    if "id" not in tmdb_df.columns:
        raise AirflowFailException("tmdb file missing 'id' column")
    tmdb_df = tmdb_df.dropna(subset=["id"])
    tmdb_df["ingestion_date"] = datetime.utcnow().date().isoformat()
    # Note: ensure your bronze.tmdb_raw table exists or ClickHouse will infer the schema
    client.insert_df("bronze.tmdb_raw", tmdb_df, column_order=list(tmdb_df.columns))

    # IMDb title.basics
    basics_file = os.path.join(RAW_DIR, "title.basics.tsv")
    basics_df = pd.read_csv(basics_file, sep="\t", na_values="\\N")
    if "tconst" not in basics_df.columns:
        raise AirflowFailException("imdb basics missing 'tconst'")
    basics_df = basics_df.dropna(subset=["tconst"])
    basics_df["ingestion_date"] = datetime.utcnow().date().isoformat()
    client.insert_df("bronze.imdb_title_basics_raw", basics_df, column_order=list(basics_df.columns))

    # IMDb title.crew
    crew_file = os.path.join(RAW_DIR, "title.crew.tsv")
    crew_df = pd.read_csv(crew_file, sep="\t", na_values="\\N")
    if "tconst" not in crew_df.columns:
        raise AirflowFailException("imdb crew missing 'tconst'")
    crew_df = crew_df.dropna(subset=["tconst"])
    crew_df["ingestion_date"] = datetime.utcnow().date().isoformat()
    client.insert_df("bronze.imdb_title_crew_raw", crew_df, column_order=list(crew_df.columns))

# --- DAG DEFAULTS ---
default_args = {
    "owner": "data-eng-team14",
    "retries": 0,
    "depends_on_past": False,
}

# --- DAG DEFINITION ---
with DAG(
    dag_id="movie_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    description="Ingest TMDb + IMDb → ClickHouse Bronze → dbt Gold",
) as dag:

    # define all tasks
    t_tmdb = PythonOperator(task_id="download_tmdb", python_callable=download_tmdb)
    t_basics = PythonOperator(task_id="download_imdb_basics", python_callable=download_imdb_basics)
    t_crew = PythonOperator(task_id="download_imdb_crew", python_callable=download_imdb_crew)
    t_load = PythonOperator(task_id="load_clickhouse_bronze", python_callable=load_clickhouse_bronze)
    
    # FIX: Use BashOperator with 'docker exec' to run dbt inside its dedicated container
    t_dbt = BashOperator(
    task_id="run_dbt_gold",
    # FIX: Change project-dir to /usr/app/dbt
    bash_command="docker exec dbt dbt run --profiles-dir /usr/app/.dbt --project-dir /usr/app/dbt --select gold"
)

# ... for t_test (run_dbt_tests)
    t_test = BashOperator(
    task_id="run_dbt_tests",
    # FIX: Change project-dir to /usr/app/dbt
    bash_command="docker exec dbt dbt test --profiles-dir /usr/app/.dbt --project-dir /usr/app/dbt --select gold"
)

    # define task order
    [t_tmdb, t_basics, t_crew] >> t_load >> t_dbt >> t_test