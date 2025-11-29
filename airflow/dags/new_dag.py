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
from airflow.operators.bash import BashOperator # Import BashOperator
from airflow.exceptions import AirflowFailException
import logging
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError # Import NoSuchTableError
import pyarrow as pa
from airflow.models.baseoperator import chain

log = logging.getLogger(__name__)

# --- ENV VARIABLES ---
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse-server")
CH_USER = os.environ.get("CLICKHOUSE_USER", "dbt_user")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "dbt_password")

KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME")
KAGGLE_KEY = os.environ.get("KAGGLE_KEY")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")

MINIO_S3_ENDPOINT = "http://minio:9000"
S3_WAREHOUSE_PATH = "s3://practice-bucket/"

# --- FILE PATHS ---
RAW_DIR = "/opt/airflow/dags/raw"
TMDB_CSV_LOCAL = "movies_tmdb.csv"

IMDB_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_CREW_URL = "https://datasets.imdbws.com/title.crew.tsv.gz"
IMDB_NAMES_URL = "https://datasets.imdbws.com/name.basics.tsv.gz"


# --- HELPERS ---
def ensure_raw_dir():
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR, exist_ok=True)
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
    
    if os.path.exists(zip_path):
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

    df = pd.read_csv(tmdb_path, low_memory=False) 

    cols = [
        "imdb_id", "title", "release_date", "production_companies",
        "budget", "revenue", "vote_average", "original_language",
        "vote_count", "runtime", "genres", "status"
    ]
    df = df[[c for c in df.columns if c in cols]]

    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df = df.dropna(subset=["imdb_id", "title", "release_date"])

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


# --- TASK 4: ICEBERG BRONZE TABLE ---
def load_iceberg_bronze(**context):
    log.info("=== Starting Iceberg Bronze Table Creation (REST + MinIO) ===")
    
    if not all([MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
        raise ValueError("Missing MinIO S3 credentials.")

    iceberg_warehouse = S3_WAREHOUSE_PATH
    table_name = os.environ.get("ICEBERG_TABLE_NAME", "tmdb_bronze") 
    
    namespace = "bronze"
    full_table_name = f"{namespace}.{table_name}"

    catalog_config = {
        "type": "rest",
        "uri": "http://iceberg-rest:8181", 
        "warehouse": iceberg_warehouse,
        "io.impl": "pyiceberg.io.pyarrow.S3FileIO",
        "s3.endpoint": MINIO_S3_ENDPOINT,
        "s3.access-key-id": MINIO_ACCESS_KEY,
        "s3.secret-access-key": MINIO_SECRET_KEY,
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
        # Removed "py-iceberg.downcast-ns-timestamp-to-us-on-write": "true" 
        # to apply explicit PyArrow casting instead for reliability.
    }

    catalog = load_catalog("rest", **catalog_config)

    try:
        catalog.create_namespace(namespace=namespace)
        log.info(f"Namespace '{namespace}' created.")
    except NamespaceAlreadyExistsError:
        log.info(f"Namespace '{namespace}' already exists.")

    tmdb_path = os.path.join(RAW_DIR, TMDB_CSV_LOCAL)
    df = pd.read_csv(tmdb_path, low_memory=False).head(5000) 

    df = df.rename(columns={'vote_average': 'vote_avg', 'id': 'tmdb_id'})
    required_cols = ["imdb_id", "title", "vote_avg", "vote_count", "release_date", "revenue"]
    df = df[[c for c in required_cols if c in df.columns]].dropna()
    
    
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    
    # FIX 1: Ensure Integer (int32) and Float (float64/double) precision
    df['vote_count'] = df['vote_count'].astype(pd.Int32Dtype())
    df['revenue'] = df['revenue'].astype(float)

    pa_table = pa.Table.from_pandas(df, preserve_index=False)
    
    # --- FIX 2: Explicitly cast release_date to microsecond precision for Iceberg ---
    log.info("Casting 'release_date' column in PyArrow table to timestamp[us].")
    pa_table = pa_table.set_column(
        pa_table.schema.get_field_index('release_date'),
        'release_date',
        pa_table['release_date'].cast(pa.timestamp('us'))
    )
    # --- END FIX 2 ---
    
    iceberg_schema = Schema(
        NestedField(1, "imdb_id", StringType(), required=False),
        NestedField(2, "title", StringType(), required=False),
        NestedField(3, "vote_avg", DoubleType(), required=False),
        NestedField(4, "vote_count", IntegerType(), required=False),
        NestedField(5, "release_date", TimestampType(), required=False), 
        NestedField(6, "revenue", DoubleType(), required=False) 
    )

    try:
        catalog.drop_table(full_table_name)
        log.info(f"Existing table {full_table_name} found and dropped.")
    except NoSuchTableError:
        log.info(f"Table {full_table_name} does not exist, proceeding with creation.")
    except Exception as e:
        log.error(f"Unexpected error during drop: {e}")
        raise

    table = catalog.create_table(full_table_name, iceberg_schema)

    # Both explicit type casting steps are now complete before the append.
    table.append(pa_table)

    log.info(f"Iceberg table created: {full_table_name}")
    log.info(f"Location: {table.location}")

    return table.location()


# --- TASK 5: LOAD INTO CLICKHOUSE ---
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
    
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df = df.dropna(subset=["release_date"])

    # Step 2: convert to python date
    df["release_date"] = df["release_date"].dt.date

    print("Clearing bronze.tmdb_raw for idempotent load")
    client.command("TRUNCATE TABLE IF EXISTS bronze.tmdb_raw")
    client.insert_df("bronze.tmdb_raw", df)
    print(f"Inserted {len(df)} TMDb rows into bronze.tmdb_raw")

    # --- IMDb loading ---
    basics_file = os.path.join(RAW_DIR, "title.basics.tsv")
    client.command("TRUNCATE TABLE bronze.imdb_title_basics_raw")
    print("Loading title.basics.tsv...")
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
    print("Loading title.crew.tsv...")
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
    print("Loading name.basics.tsv...")
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


# --- TASK 6: DBT GOLD + TESTS ---
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
            print(f"Warning: Could not drop {table}: {e}")

    cmd = [
        "dbt", "run",
        "--project-dir", "/opt/airflow/project_root/dbt",
        "--profiles-dir", "/opt/airflow/project_root/dbt",
        "--select", "gold"
    ]
    env = os.environ.copy()
    env["DBT_SCHEMA"] = "gold"
    
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)

    if result.returncode != 0:
        raise AirflowFailException(f"dbt run failed:\n{result.stdout}\n{result.stderr}")
    
    print("dbt Gold run successful.")
    return result.stdout


def run_dbt_tests(**context):
    cmd = [
        "dbt", "test",
        "--project-dir", "/opt/airflow/project_root/dbt",
        "--profiles-dir", "/opt/airflow/project_root/dbt",
        "--select", "gold"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    
    if result.returncode != 0:
        raise AirflowFailException(f"dbt test failed:\n{result.stdout}\n{result.stderr}")
    
    print("dbt tests successful.")
    return result.stdout

# --- TASK 7: DBT user and role creation for Clickhouse ---
def run_CH_roles_users(**context):
    client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)
    statements = [

        "CREATE ROLE IF NOT EXISTS analyst_full",
        "CREATE ROLE IF NOT EXISTS analyst_limited",
        "CREATE USER IF NOT EXISTS npc123 IDENTIFIED BY 'user'",
        "CREATE USER IF NOT EXISTS bigbo55 IDENTIFIED BY 'admin'"
    ]
    for sql in statements:
        client.command(sql)
        print(f"Executed: {sql}")

# --- TASK 8: DBT grant roles and view access for Clickhouse users ---
def run_CH_grants(**context):
    client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)
    statements = [
        "GRANT SELECT (vote_avg, vote_count, revenue, budget, movie_popularity, revenue_growth, popularity_change, vote_avg_change, "
        "date_id, director_id, genre_id, imdb_id, production_id) ON gold.fact_movie_performance TO analyst_limited",
        
        "GRANT SELECT (movie_title, imdb_id) ON gold.dim_movie TO analyst_limited",
        "GRANT SELECT (year, date_id) ON gold.dim_date TO analyst_limited",
        "GRANT SELECT (director_name, director_id) ON gold.dim_director TO analyst_limited",
        "GRANT SELECT (genre_name, genre_id) ON gold.dim_genre TO analyst_limited",
        "GRANT SELECT (production_name, production_id) ON gold.dim_production TO analyst_limited",
        "GRANT SELECT ON gold.analytical_view_limited TO analyst_limited",

        "GRANT SELECT ON gold.fact_movie_performance TO analyst_full",
        "GRANT SELECT ON gold.dim_movie TO analyst_full",
        "GRANT SELECT ON gold.dim_date TO analyst_full",
        "GRANT SELECT ON gold.dim_director TO analyst_full",
        "GRANT SELECT ON gold.dim_genre TO analyst_full",
        "GRANT SELECT ON gold.dim_production TO analyst_full",
        "GRANT SELECT ON gold.analytical_view_full TO analyst_full",
        "GRANT SELECT ON gold.analytical_view_limited TO analyst_full",

        "GRANT analyst_full TO bigbo55",
        "GRANT analyst_limited TO npc123"
    ]

    for sql in statements:
        client.command(sql)
        print(f"Executed: {sql}")

# --- DAG CONFIG ---
default_args = {"owner": "data-eng-team14", "retries": 0, "depends_on_past": False}

with DAG(
    dag_id="new_movie_pipeline",
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
    
    # --- MinIO Bucket Creation Task (Fixed) ---
    t_setup_bucket = BashOperator(
        task_id="setup_minio_bucket",
        # FIX: Set HOME, USER, and MC_CONFIG_DIR explicitly to bypass the dependency on missing 'getent'
        bash_command="""
# Provide dummy environment variables to bypass 'getent' lookup failures.
export HOME=/tmp
export USER=airflow

# Set MC_CONFIG_DIR to a temporary folder to store config
mkdir -p /tmp/mc_config
export MC_CONFIG_DIR=/tmp/mc_config

echo "Configuring MinIO client 'local'..."
mc alias set local http://minio:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}

echo "Checking if bucket 'practice-bucket' exists..."
if mc ls local/practice-bucket > /dev/null 2>&1; then
  echo "Bucket 'practice-bucket' already exists."
else
  echo "Bucket 'practice-bucket' not found. Creating..."
  mc mb local/practice-bucket
fi
        """,
        env={
            # Pass credentials securely to the BashOperator environment
            "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
            "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
        }
    )
    
    t_iceberg = PythonOperator(task_id="load_iceberg_bronze", python_callable=load_iceberg_bronze)
    
    t_basics = PythonOperator(task_id="download_imdb_basics", python_callable=download_imdb_basics)
    t_crew = PythonOperator(task_id="download_imdb_crew", python_callable=download_imdb_crew)
    t_names = PythonOperator(task_id="download_imdb_names", python_callable=download_imdb_names)
    
    t_load = PythonOperator(task_id="load_clickhouse_bronze", python_callable=load_clickhouse_bronze)
    
    t_dbt = PythonOperator(task_id="run_dbt_gold", python_callable=run_dbt_gold)
    t_test = PythonOperator(task_id="run_dbt_tests", python_callable=run_dbt_tests)

    t_roles = PythonOperator(task_id="run_CH_roles_users", python_callable=run_CH_roles_users)
    t_grants = PythonOperator(task_id="run_CH_grants", python_callable=run_CH_grants)

    # --- UPDATED DEPENDENCIES ---
    
    # 1. TMDb flow: Filter, then setup bucket, then load Iceberg
    chain(t_tmdb, t_filter, t_setup_bucket, t_iceberg)
    
    # 2. IMDb downloads to ClickHouse raw tables
    chain([t_basics, t_crew, t_names], t_load)
    
    # 3. All bronze creation must complete before dbt starts. After tests add roles and grants.
    chain([t_iceberg, t_load], t_dbt, t_test, t_roles, t_grants)
