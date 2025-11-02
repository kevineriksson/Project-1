# Project 2 — Automated Movie Data Warehouse (Airflow + dbt + ClickHouse)

## Overview

This project automates the movie analytics pipeline from **Project 1**.
It leverages **Apache Airflow** for orchestration, **dbt** for data transformations, and **ClickHouse** as the data warehouse.

The pipeline:

* Downloads and ingests IMDb and TMDb data
* Cleans and transforms it through **bronze → silver → gold** layers
* Builds a **star schema** under the `_gold` schema
* Runs automatically using Airflow

---

## Business Goal

Automate a reproducible movie analytics workflow to:

1. Ingest IMDb and TMDb datasets
2. Transform raw data into clean, analytics-ready tables
3. Build a star schema for reporting
4. Enable repeatable, scheduled pipeline runs via Airflow

---

## Datasets

* **TMDb Movies Dataset (Kaggle)** — movie metadata: title, release date, budget, revenue, etc.
* **IMDb Datasets** — core movie IDs, ratings, votes, crew, and genres

---

## Tools

* **Docker Compose** — containerized environment
* **Apache Airflow** — orchestrates ETL tasks
* **dbt (Data Build Tool)** — transformations & testing
* **ClickHouse** — analytics data warehouse
* **Kaggle API** — automated dataset download

---

## Setup & Environment

### 1. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# .env
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_api_key
AIRFLOW_UID=50000
```

---

### 2. Start Docker Environment

Run:

```bash
docker compose up -d
```

Containers started:

* `clickhouse-server` — ClickHouse database
* `airflow-webserver` — Airflow UI
* `airflow-scheduler` — Airflow scheduler
* `dbt` — dbt transformation container

Access the Airflow UI at [http://localhost:8080](http://localhost:8080)

```
Username: airflow
Password: airflow
```

Wait until all containers are **healthy**.

---

### 3. Run the Pipeline

1. Open the Airflow UI.
2. Find the DAG named `movie_pipeline`.
3. Enable the DAG.
4. Trigger the DAG manually or schedule it.
5. Monitor task completion.

Pipeline tasks:

1. Download IMDb and TMDb datasets via Kaggle API
2. Load raw data into ClickHouse (**bronze layer**)
4. Transform and clean data. Build final star schema in `_gold` (**gold layer**)
5. Run dbt tests for data quality

---

### 4. Notes & Tips

* Ensure Kaggle API credentials are correct.
* Confirm Docker containers are running before triggering Airflow.
* Inspect ClickHouse tables:

```bash
clickhouse-client --query "SHOW TABLES FROM _gold"
```

* All transformations are defined in dbt models for reproducibility and testing.
* In case of task failure during the movie_pipeline DAG it is most likely an out of memory error.
  To fix it, increase memory resource allocation in Docker, or stop any resource heavy applications during task activation.
  When using wsl, it might help to create a .wslconfig file in your user folder with the following content based on your system memory:
  
  [wsl2]
  memory=6GB
  processors=4
  swap=2GB
  localhostForwarding=true

  ---

  ### 5. Airflow DAGs
  <img width="1892" height="605" alt="DAG_1" src="https://github.com/user-attachments/assets/13dfdce1-86f3-4823-8309-6d6a39bde50f" />
  
  <img width="1896" height="895" alt="DAG_2" src="https://github.com/user-attachments/assets/a0e9750e-6d54-47f5-88a2-2f7fe12e7bf5" />

  ---

  ### 6. Example query results
  * Which genres have the highest average ratings?
  <img width="1650" height="583" alt="Q1" src="https://github.com/user-attachments/assets/f980fc71-f836-453a-90ed-7e6cf2d0a656" />

  * Which directors consistently produce high-rated movies and high revenue?
  <img width="1650" height="668" alt="Q2" src="https://github.com/user-attachments/assets/f4de6c6b-7693-4502-bf8e-3a9570741e75" />

  * How does average rating correlate with box-office revenue across release years?
  <img width="1654" height="672" alt="Q3" src="https://github.com/user-attachments/assets/6aa62e69-b5b6-4f9a-a206-c7920e5ccfd2" />

  * What are the top 10 movies by revenue per genre for a given year?
    
  <img width="1642" height="822" alt="Q4" src="https://github.com/user-attachments/assets/e71a0fac-50f2-4dfb-b1ea-b35191ae7b47" />

  * How does the runtime affect changes to the overall box-office revenue?
    
  <img width="1650" height="674" alt="Q5" src="https://github.com/user-attachments/assets/0217843b-5ae4-44c4-9a5e-db00b74a4883" />


  
