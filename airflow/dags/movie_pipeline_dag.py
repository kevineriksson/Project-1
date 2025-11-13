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
import shutil
from airflow import DAG
from airflow.utils.dates import days_ago
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


# --- TASK 1: DOWNLOAD TMDb ---
def download_tmdb(**kwargs):
    ensure_raw_dir()
    zip_path = os.path.join(RAW_DIR, "tmdb_movies.zip")
    url = "https://www.kaggle.com/api/v1/datasets/download/asaniczka/tmdb-movies-dataset-2023-930k-movies"

    run_date = kwargs.get("ds", datetime.utcnow().strftime("%Y-%m-%d"))
    versioned_name = f"movies_tmdb_{run_date}.csv"
    versioned_path = os.path.join(RAW_DIR, versioned_name)
    final_path = os.path.join(RAW_DIR, TMDB_CSV_LOCAL)

    if not KAGGLE_USERNAME or not KAGGLE_KEY:
        raise AirflowFailException("Missing KAGGLE_USERNAME or KAGGLE_KEY environment variables.")

    print(f"Downloading TMDb dataset for run date {run_date} ...")
    resp = requests.get(url, auth=(KAGGLE_USERNAME, KAGGLE_KEY), stream=True, timeout=120)
    if resp.status_code != 200:
        raise AirflowFailException(f"Failed to download TMDb dataset. Status: {resp.status_code}")

    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    print("Download complete.")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(RAW_DIR)
    os.remove(zip_path)

    csv_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]
    if not csv_files:
        raise AirflowFailException("No CSV file found in TMDb dataset ZIP")

    src = os.path.join(RAW_DIR, csv_files[0])
    os.rename(src, versioned_path)
    shutil.copy(versioned_path, final_path)

    print(f"Saved versioned file: {versioned_path}")
    print(f"Standardized file for downstream steps: {final_path}")
    return final_path


# --- TASK 2: FILTER TMDb BEFORE LOADING ---
def filter_tmdb(**context):
    tmdb_path = os.path.join(RAW_DIR, TMDB_CSV_LOCAL)
    print(f"Filtering TMDb file: {tmdb_path}")

    df = pd.read_csv(tmdb_path)

    # Keep only relevant fields
    cols = [
        "imdb_id", "title", "release_date", "production_companies",
        "budget", "revenue", "vote_average", "original_language",
        "vote_count", "runtime", "genres", "status"
    ]
    df = df[[c for c in df.columns if c in cols]]

    # Parse dates
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df = df.dropna(subset=["imdb_id", "title", "release_date"])

    # Business filters
    df = df[
        (df["release_date"].dt.year >= 2000)
        & (df["status"].fillna("").str.lower() == "released")
        & (pd.to_numeric(df["runtime"], errors="coerce").fillna(0) > 40)
        & (pd.to_numeric(df["vote_average"], errors="coerce").fillna(0) > 0)
        & (pd.to_numeric(df["vote_count"], errors="coerce").fillna(0) > 0)
    ]

    print(f"Filtered down to {len(df)} valid movies after quality filters.")
    df.to_csv(tmdb_path, index=False)
    print("Rewrote TMDb file in-place (filtered).")
    return tmdb_path


# --- TASK 3: DOWNLOAD IMDb FILES ---
def _download_gz_tsv(url, out_name):
    out_path = os.path.join(RAW_DIR, out_name)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz, open(out_path, "wb") as f_out:
            for chunk in iter(lambda: gz.read(8192), b""):
                f_out.write(chunk)
    return out_path


def download_imdb_basics(**context):
    return _download_gz_tsv(IMDB_BASICS_URL, "title.basics.tsv")


def download_imdb_crew(**context):
    return _download_gz_tsv(IMDB_CREW_URL, "title.crew.tsv")


def download_imdb_names(**context):
    return _download_gz_tsv(IMDB_NAMES_URL, "name.basics.tsv")


# --- TASK 4: LOAD INTO CLICKHOUSE ---
def load_clickhouse_bronze(**context):
    client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)
    client.command("SET max_insert_block_size = 500000")

    # --- Load filtered TMDb ---
    tmdb_path = os.path.join(RAW_DIR, TMDB_CSV_LOCAL)
    df = pd.read_csv(tmdb_path)

    expected_cols = [
        "imdb_id", "title", "release_date", "production_companies",
        "budget", "revenue", "vote_average", "original_language",
        "vote_count", "runtime", "genres"
    ]
    df = df[[c for c in df.columns if c in expected_cols]]
    for c in ["imdb_id", "title", "production_companies", "genres", "original_language"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date

    print("Clearing bronze.tmdb_raw for idempotent load")
    client.command("TRUNCATE TABLE IF EXISTS bronze.tmdb_raw")
    client.insert_df("bronze.tmdb_raw", df)
    print(f"Inserted {len(df)} TMDb rows into bronze.tmdb_raw")

    # --- IMDb loading (exactly like old stable version) ---
    basics_file = os.path.join(RAW_DIR, "title.basics.tsv")
    client.command("TRUNCATE TABLE bronze.imdb_title_basics_raw")
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
        client.insert_df("bronze.imdb_title_basics_raw", chunk)
        print(f"Inserted title.basics chunk {i+1} ({len(chunk)} rows)")

    crew_file = os.path.join(RAW_DIR, "title.crew.tsv")
    client.command("TRUNCATE TABLE bronze.imdb_title_crew_raw")
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

    names_file = os.path.join(RAW_DIR, "name.basics.tsv")
    client.command("TRUNCATE TABLE bronze.imdb_name_basics_raw")
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


# --- TASK 5: DBT GOLD + TESTS ---
def run_dbt_gold(**context):
    client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)

    tables_to_drop = [
        "dim_director",
        "dim_genre",
        "dim_movie",
        "dim_production",
        "dim_date",
        "fact_movie_performance"
    ]

    for table in tables_to_drop:
        try:
            client.command(f"DROP TABLE IF EXISTS gold.{table} SYNC")
            print(f"Dropped table gold.{table}")
        except Exception as e:
            print(f"Could not drop {table}: {e}")

    cmd = [
        "dbt", "run",
        "--project-dir", "/opt/airflow/project_root/dbt",
        "--profiles-dir", "/opt/airflow/project_root/dbt",
        "--select", "gold"
    ]
    env = os.environ.copy()
    env["DBT_SCHEMA"] = "gold"
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    if result.returncode != 0:
        raise AirflowFailException(f"dbt run failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout


def run_dbt_tests(**context):
    cmd = [
        "dbt", "test",
        "--project-dir", "/opt/airflow/project_root/dbt",
        "--profiles-dir", "/opt/airflow/project_root/dbt",
        "--select", "gold"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise AirflowFailException(f"dbt test failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout

# --- DAG CONFIG ---
default_args = {"owner": "data-eng-team14", "retries": 0, "depends_on_past": False}

with DAG(
    dag_id="movie_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    description="Download → Filter → Bronze → dbt Gold",
) as dag:

    t_tmdb = PythonOperator(task_id="download_tmdb", python_callable=download_tmdb)
    t_filter = PythonOperator(task_id="filter_tmdb", python_callable=filter_tmdb)
    t_basics = PythonOperator(task_id="download_imdb_basics", python_callable=download_imdb_basics)
    t_crew = PythonOperator(task_id="download_imdb_crew", python_callable=download_imdb_crew)
    t_names = PythonOperator(task_id="download_imdb_names", python_callable=download_imdb_names)
    t_load = PythonOperator(task_id="load_clickhouse_bronze", python_callable=load_clickhouse_bronze)
    t_dbt = PythonOperator(task_id="run_dbt_gold", python_callable=run_dbt_gold)
    t_test = PythonOperator(task_id="run_dbt_tests", python_callable=run_dbt_tests)

    t_tmdb >> t_filter
    [t_filter, t_basics, t_crew, t_names] >> t_load >> t_dbt >> t_test
