# --- IMPORTS ---
from datetime import datetime, timedelta
import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import clickhouse_connect



# --- ENV VARIABLES ---
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse-server")
CH_USER = os.environ.get("CLICKHOUSE_USER", "dbt_user")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "dbt_password")

KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME")
KAGGLE_KEY = os.environ.get("KAGGLE_KEY")

# --- FILE PATHS ---
RAW_DIR = "/opt/airflow/dags/raw"
os.makedirs(RAW_DIR, exist_ok=True)


# --- TASK 1: RUN DBT TRANSFORMATIONS ---
def run_dbt_gold(**context):
    # Drop existing tables before running dbt
    client = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS)
    
    tables_to_drop = [
    "dim_director",
    "dim_genre",
    "dim_movie",
    "dim_production",
    "dim_date",
    "fact_movie"
]

    for table in tables_to_drop:
        try:
            client.command(f"DROP TABLE IF EXISTS gold.{table} SYNC")  # remove double gold
            print(f"Dropped table gold.{table}")
        except Exception as e:
            print(f"Could not drop {table}: {e}")
        
    # Now run dbt
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


# --- TASK 2: RUN DBT TESTS ---
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
    description="dbt Gold transformations and tests",
) as dag:

    t_dbt = PythonOperator(task_id="run_dbt_gold", python_callable=run_dbt_gold)
    t_test = PythonOperator(task_id="run_dbt_tests", python_callable=run_dbt_tests)

    t_dbt >> t_test