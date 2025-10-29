# --- IMPORTS ---
from datetime import datetime, timedelta
import os
import pandas as pd
import gzip
import io
import requests
import subprocess
import clickhouse_connect
import zipfile # <-- NEW IMPORT
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


# --- ENV VARIABLES (from docker-compose) ---
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse-server")
CH_USER = os.environ.get("CLICKHOUSE_USER", "dbt_user")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "dbt_pass")

KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME")
KAGGLE_KEY = os.environ.get("KAGGLE_KEY")


# --- FILE PATHS ---
RAW_DIR = "/opt/airflow/dags/raw"
os.makedirs(RAW_DIR, exist_ok=True)
TMDB_CSV_LOCAL = "movies_tmdb.csv"

IMDB_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_CREW_URL = "https://datasets.imdbws.com/title.crew.tsv.gz"
IMDB_NAMES_URL = "https://datasets.imdbws.com/name.basics.tsv.gz" # <-- NEW URL


# --- HELPER: ENSURE RAW DIRECTORY EXISTS ---
def ensure_raw_dir():
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR, exist_ok=True)


# --- TASK 1: DOWNLOAD TMDb DATASET FROM KAGGLE (FIXED) ---
def download_tmdb(**context):
    ensure_raw_dir()
    zip_path = os.path.join(RAW_DIR, "tmdb_movies.zip")
    out_path = os.path.join(RAW_DIR, TMDB_CSV_LOCAL)

    url = "https://www.kaggle.com/api/v1/datasets/download/asaniczka/tmdb-movies-dataset-2023-930k-movies"

    # ensure credentials are available
    if not KAGGLE_USERNAME or not KAGGLE_KEY:
        raise AirflowFailException("Missing KAGGLE_USERNAME or KAGGLE_KEY environment variables.")

    # 1. Download the file using requests with basic auth (Replaces curl)
    print(f"Downloading TMDb dataset from Kaggle to {zip_path}...")
    # Use basic authentication (username:key) for Kaggle API
    resp = requests.get(url, auth=(KAGGLE_USERNAME, KAGGLE_KEY), stream=True, timeout=120)

    if resp.status_code != 200:
        raise AirflowFailException(
            f"Failed to download TMDb dataset. Status: {resp.status_code}. Response: {resp.text[:200]}"
        )

    # Save the downloaded content to the zip file
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    print("Download complete.")

    # 2. Unzip the file using Python's zipfile module
    print("Unzipping the file...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(RAW_DIR)
        print("Unzipping complete.")
    except Exception as e:
        raise AirflowFailException(f"Failed to unzip TMDb dataset: {e}")

    # 3. Find and rename the first CSV file
    csv_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]
    if not csv_files:
        raise AirflowFailException("No CSV file found in TMDb dataset ZIP")

    original_csv_name = csv_files[0]
    os.rename(os.path.join(RAW_DIR, original_csv_name), out_path)
    print(f"Renamed '{original_csv_name}' to '{TMDB_CSV_LOCAL}' at {RAW_DIR}")
    return out_path


# --- HELPER: DOWNLOAD AND UNPACK GZ FILES ---
def _download_gz_tsv(url, out_name):
    resp = requests.get(url, timeout=60)
    if resp.status_code != 200:
        raise AirflowFailException(f"Failed to download {url}")
    with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
        # Note: Added explicit errors="replace" for robust TSV reading
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


# --- TASK 4: DOWNLOAD IMDb NAMES (NEW) ---
def download_imdb_names(**context):
    return _download_gz_tsv(IMDB_NAMES_URL, "name.basics.tsv")


# --- TASK 5: LOAD ALL RAW FILES TO CLICKHOUSE BRONZE (UPDATED) ---
def load_clickhouse_bronze(**context):
    client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)

    # TMDb
    tmdb_file = os.path.join(RAW_DIR, TMDB_CSV_LOCAL)
    tmdb_df = pd.read_csv(tmdb_file)
    if "id" not in tmdb_df.columns:
        raise AirflowFailException("TMDb file missing 'id' column")
    tmdb_df = tmdb_df.dropna(subset=["id"])
    tmdb_df["ingestion_date"] = datetime.utcnow().date().isoformat()
    client.insert_df("bronze.tmdb_raw", tmdb_df, column_order=list(tmdb_df.columns))

    # IMDb title.basics
    basics_file = os.path.join(RAW_DIR, "title.basics.tsv")
    basics_df = pd.read_csv(basics_file, sep="\t", na_values="\\N")
    if "tconst" not in basics_df.columns:
        raise AirflowFailException("IMDb basics missing 'tconst'")
    basics_df = basics_df.dropna(subset=["tconst"])
    basics_df["ingestion_date"] = datetime.utcnow().date().isoformat()
    client.insert_df("bronze.imdb_title_basics_raw", basics_df, column_order=list(basics_df.columns))

    # IMDb title.crew
    crew_file = os.path.join(RAW_DIR, "title.crew.tsv")
    crew_df = pd.read_csv(crew_file, sep="\t", na_values="\\N")
    if "tconst" not in crew_df.columns:
        raise AirflowFailException("IMDb crew missing 'tconst'")
    crew_df = crew_df.dropna(subset=["tconst"])
    crew_df["ingestion_date"] = datetime.utcnow().date().isoformat()
    client.insert_df("bronze.imdb_title_crew_raw", crew_df, column_order=list(crew_df.columns))

    # **NEW: IMDb name.basics**
    names_file = os.path.join(RAW_DIR, "name.basics.tsv")
    names_df = pd.read_csv(names_file, sep="\t", na_values="\\N")
    if "nconst" not in names_df.columns:
        raise AirflowFailException("IMDb names missing 'nconst'")
    names_df = names_df.dropna(subset=["nconst"])
    names_df["ingestion_date"] = datetime.utcnow().date().isoformat()
    client.insert_df("bronze.imdb_name_basics_raw", names_df, column_order=list(names_df.columns))


# --- TASK 6: RUN DBT TRANSFORMATIONS ---
def run_dbt_gold(**context):
    cmd = [
        "dbt", "run",
        "--project-dir", "/opt/dbt",
        "--profiles-dir", "/opt/dbt",
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


# --- DAG DEFAULTS ---
default_args = {
    "owner": "data-eng-team14",
    "retries": 0,
    "depends_on_past": False,
}


# --- DAG DEFINITION (UPDATED) ---
with DAG(
    dag_id="movie_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    description="Ingest TMDb + All IMDb files → ClickHouse Bronze → dbt Gold",
) as dag:

    t_tmdb = PythonOperator(task_id="download_tmdb", python_callable=download_tmdb)
    t_basics = PythonOperator(task_id="download_imdb_basics", python_callable=download_imdb_basics)
    t_crew = PythonOperator(task_id="download_imdb_crew", python_callable=download_imdb_crew)
    t_names = PythonOperator(task_id="download_imdb_names", python_callable=download_imdb_names) # <-- NEW TASK

    t_load = PythonOperator(task_id="load_clickhouse_bronze", python_callable=load_clickhouse_bronze)
    t_dbt = PythonOperator(task_id="run_dbt_gold", python_callable=run_dbt_gold)
    t_test = PythonOperator(task_id="run_dbt_tests", python_callable=run_dbt_tests)

    # All 4 download tasks must complete before the load task runs
    [t_tmdb, t_basics, t_crew, t_names] >> t_load >> t_dbt >> t_test