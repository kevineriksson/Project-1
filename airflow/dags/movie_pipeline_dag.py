# --- IMPORTS ---
from datetime import datetime, timedelta
import os
import pandas as pd
import gzip
import io
import requests
import subprocess
import clickhouse_connect


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


# --- ENV VARIABLES (from docker-compose) ---
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_USER = os.environ.get("CLICKHOUSE_USER", "analytics")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "analytics_pw")


# --- FILE PATHS ---
RAW_DIR = "/opt/airflow/dags/raw"
os.makedirs(RAW_DIR, exist_ok=True)


TMDB_SOURCE = "movies_tmdb.csv"
IMDB_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_CREW_URL = "https://datasets.imdbws.com/title.crew.tsv.gz"


# --- TASK 1: VERIFY OR DOWNLOAD TMDB ---
def download_tmdb(**context):
   tmdb_path = os.path.join(RAW_DIR, TMDB_SOURCE)
   if not os.path.exists(tmdb_path):
       raise AirflowFailException(f"TMDb source file not found at {tmdb_path}")
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


# --- TASK 4: LOAD ALL RAW FILES TO CLICKHOUSE BRONZE ---
def load_clickhouse_bronze(**context):
   client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)


   # TMDb
   tmdb_file = os.path.join(RAW_DIR, TMDB_SOURCE)
   tmdb_df = pd.read_csv(tmdb_file)
   if "id" not in tmdb_df.columns:
       raise AirflowFailException("tmdb file missing 'id' column")
   tmdb_df = tmdb_df.dropna(subset=["id"])
   tmdb_df["ingestion_date"] = datetime.utcnow().date().isoformat()
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


# --- TASK 5: RUN DBT TRANSFORMATIONS (BUILD GOLD) ---
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


# --- TASK 6: RUN DBT TESTS ---
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
   t_dbt = PythonOperator(task_id="run_dbt_gold", python_callable=run_dbt_gold)
   t_test = PythonOperator(task_id="run_dbt_tests", python_callable=run_dbt_tests)


   # define task order
   [t_tmdb, t_basics, t_crew] >> t_load >> t_dbt >> t_test
