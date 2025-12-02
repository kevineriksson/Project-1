# Project 3 — Automated Movie Data Warehouse (Airflow + dbt + ClickHouse + Iceberg + Open Metadata + Superset)

## Overview

This project automates the movie analytics pipeline from **Project 1**.
It leverages **Apache Airflow** for orchestration, **dbt** for data transformations, and **ClickHouse** as the data warehouse.

The pipeline:

* Downloads and ingests IMDb and TMDb data
* Cleans and transforms it through **bronze → silver → gold** layers
* Builds a **star schema** under the `gold` schema
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
* **OpenMetadata** — data quality tests and column descriptors
* **Apache Superset** — dashboard based on the gold models

---

## Setup & Environment

### 1. Configure Environment Variables

Create a `.env` file in the project root like the example .env file, adding also your Kaggle credentials:

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

In case of any errors with running the containers like elasticsearch, compose it again or do a:

```bash
docker compose down -v
```
followed by another:

```bash
docker compose up -d
```


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
4. Transform and clean data. Build final star schema in `gold` (**gold layer**)
5. Run dbt tests for data quality

---

### 4. Notes & Tips

* Ensure Kaggle API credentials are correct.
* Confirm Docker containers are running before triggering Airflow.
* Enter the credentials and inspect ClickHouse tables:

```
Username: dbt_user
Password: dbt_password
```

```bash
clickhouse-client --query "SHOW TABLES FROM gold"
```

* All transformations are defined in dbt models for reproducibility and testing.
* In case of task failure during the movie_pipeline DAG it is most likely an out of memory error.
  To fix it, increase memory resource allocation in Docker, or stop any resource heavy applications during task activation.
  When using wsl, it might help to create a .wslconfig file in your user folder with the following content based on your system memory:
  
  ```
  [wsl2]
  memory=6GB
  processors=4
  swap=2GB
  localhostForwarding=true
  ```
* When task 7 or 8 (creating users and roles) fail due to access denied errors - remove the two tasks from the dag along with their dependency chain and
  definitions. Then run the sql for the users manually from the sql files in the sql folder to test querying the views.
  
* It also may happen that the computer runs out of memory and some tasks fail. To counteract this, when running DAGs in Airflow, disable Superset and Open
  Metadata containers as those take the most memory.
  
  ---

### 5. Airflow DAGs
<img width="1892" height="605" alt="DAG_1" src="https://github.com/user-attachments/assets/13dfdce1-86f3-4823-8309-6d6a39bde50f" />
  
<img width="1896" height="895" alt="DAG_2" src="https://github.com/user-attachments/assets/a0e9750e-6d54-47f5-88a2-2f7fe12e7bf5" />

  ---
### 6. DBT graph / lineage
```
    Sources (Bronze)
├── imdb_name_basics_raw
├── imdb_title_basics_raw
├── imdb_title_crew_raw
└── tmdb_raw
       ↓
Analytics Layer (Gold / gold)
├── dim_movie
├── dim_genre
├── dim_director
├── dim_production
├── dim_date
└── fact_movie_performance
```
  ---

### 7. Example query results
* Which genres have the highest average ratings?
```sql
SELECT
    g.genre_name,
    round(AVG(fm.vote_avg), 2) AS avg_rating
FROM gold.fact_movie_performance AS fm
INNER JOIN gold.dim_genre AS g
    ON fm.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC;
```
<img width="1820" height="658" alt="Q1" src="https://github.com/user-attachments/assets/40340f45-9699-475f-9ad8-5461912948f0" />



* Which directors consistently produce high-rated movies and high revenue?
```sql
SELECT
    d.director_name,
    round(AVG(fm.vote_avg), 2) AS avg_rating,
    round(AVG(fm.revenue), 2) AS avg_revenue,
    COUNT(fm.movie_id) AS movie_count
FROM gold.fact_movie_performance AS fm
INNER JOIN gold.dim_director AS d
    ON fm.director_id = d.director_id
GROUP BY d.director_name
HAVING 
    COUNT(fm.movie_id) >= 5
    AND AVG(fm.vote_avg) > 6.5
    AND AVG(fm.revenue) > 10000000
ORDER BY avg_rating DESC, avg_revenue DESC
LIMIT 10;
```
<img width="1842" height="527" alt="Q2" src="https://github.com/user-attachments/assets/41a98703-1181-4d5d-95eb-b612a137b981" />



* How does average rating correlate with box-office revenue across release years?
```sql
SELECT
    toYear(m.release_date) AS release_year,
    round(AVG(fm.vote_avg), 2) AS avg_rating,
    round(AVG(fm.revenue), 0) AS avg_revenue
FROM gold.fact_movie_performance AS fm
INNER JOIN gold.dim_movie AS m
    ON fm.movie_id = m.imdb_id
GROUP BY release_year
ORDER BY release_year ASC;
```
<img width="1830" height="813" alt="Q3" src="https://github.com/user-attachments/assets/292904b2-8718-499e-bee7-78843aeacbe6" />



* What are the top movies by revenue per genre for a given year?
```sql
SELECT
    g.genre_name,
    anyHeavy(m.movie_title) AS top_movie,
    max(fm.revenue) AS top_revenue
FROM gold.fact_movie_performance AS fm
INNER JOIN gold.dim_movie AS m
    ON fm.movie_id = m.imdb_id
INNER JOIN gold.dim_genre AS g
    ON fm.genre_id = g.genre_id
WHERE toYear(m.release_date) = 2005
GROUP BY g.genre_name
ORDER BY top_revenue DESC
LIMIT 10;
```
<img width="1833" height="403" alt="Q4" src="https://github.com/user-attachments/assets/5e3ccf5e-04a8-4ddc-9753-8412ea2d6e51" />



* How does the runtime affect changes to the overall box-office revenue?
```sql
SELECT
    multiIf(
        m.movie_runtime <= 60, '0-60 min',
        m.movie_runtime <= 90, '61-90 min',
        m.movie_runtime <= 120, '91-120 min',
        m.movie_runtime <= 150, '121-150 min',
        '151+ min'
    ) AS runtime_bucket,
    COUNT(fm.movie_id) AS movie_count,
    AVG(fm.revenue) AS avg_revenue
FROM gold.fact_movie_performance AS fm
INNER JOIN gold.dim_movie AS m
    ON fm.movie_id = m.imdb_id
WHERE m.movie_runtime IS NOT NULL
  AND fm.revenue IS NOT NULL
GROUP BY runtime_bucket
ORDER BY avg_revenue ASC;
```
<img width="1826" height="236" alt="Q5" src="https://github.com/user-attachments/assets/c9c8b245-cc5e-43f1-afe4-e17384eb5b5e" />

---

### 8. Apache Iceberg Integration

* A new Airflow task load_iceberg_bronze was added to write filtered TMDb movie data into an Apache Iceberg table stored in MinIO (S3-compatible storage).

* The task creates the Iceberg namespace and table via the REST catalog and stores Parquet + metadata files under: s3://practice-bucket/bronze/tmdb_bronze/

* The DAG now includes the full flow:
Download → Filter → MinIO Bucket Setup → Iceberg Write → Bronze / Gold Transformations

* ClickHouse OSS does not include the ENGINE = Iceberg table engine.
To make Iceberg data queryable in ClickHouse, the Iceberg Parquet output files were exposed using the S3 table engine.

* External table definition used to read the Iceberg data from MinIO:
 ```sql 
CREATE DATABASE IF NOT EXISTS bronze;
DROP TABLE IF EXISTS bronze.tmdb_bronze_ch SYNC;
CREATE TABLE bronze.tmdb_bronze_ch
(
    imdb_id String,
    title String,
    vote_avg Float64,
    vote_count Int32,
    release_date DateTime64,
    revenue Float64
)
ENGINE = S3(
    'http://minio:9000/practice-bucket/bronze/tmdb_bronze/data/00000-0-30f80b1c-921b-4022-b9ed-4a2949350601.parquet',
    'minioadmin',
    'minioadmin',
    'Parquet'
)
SETTINGS
    s3_use_environment_credentials = 0,
    s3_region = 'us-east-1';
```
<img width="1804" height="768" alt="image" src="https://github.com/user-attachments/assets/60dcb654-85b9-4a83-a622-941a608a8d52" />

### 9. Data masking implementation in ClickHouse

The dbt setup now includes 2 additional views based on fact_movie_performance, queriable in ClickHouse:
* analytical_view_full - data is unchanged
* analytical_view_limited - with 4 masked columns: movie_title, release_date, director_name, and vote_avg

The sql for view creation and masking logic can be found in dbt -> models -> gold -> views.

Additionally, for automation purposes, there are two extra tasks within the DAG - Task 7 and 8 for creating users, roles and giving them the required GRANTs.
Still, it could be that running the tasks will result in an access denied error on some computers, so in that case, remove the two tasks from the dag along with their dependency chain and definitions. Then run the sql for the users manually from the sql files in the sql folder to see the same results.

* Limited user login:
```
Username: npc123
Password: user
```

* Full access user:
```
Username: bigbo55
Password: admin
```

* Limited access user accessing the masked view:

<img width="1835" height="882" alt="view_1" src="https://github.com/user-attachments/assets/c11dcf38-3578-4660-a027-d6adb7818685" />

* Limited access user accessing the unmasked view:

<img width="1851" height="392" alt="view_2" src="https://github.com/user-attachments/assets/73927baa-0a83-4235-8e47-eec1685d0990" />

* Full access user accessing the unmasked view:

<img width="1832" height="873" alt="view_3" src="https://github.com/user-attachments/assets/5b9aabda-eb04-4cdb-9eb6-eb073cdba5c6" />

### 10. OpenMetadata Integration**

This project uses **OpenMetadata** as the data governance layer to document datasets, run data quality tests, and catalog analytical assets.

### **Accessing OpenMetadata**

Once the project is running:

```bash
docker compose up -d
```

Open OpenMetadata in your browser:

```
http://localhost:8585
```

**Login credentials:**

```
Username: admin@open-metadata.org
Password: admin
```

After logging in, this step registers the ClickHouse data source with OpenMetadata.

Go to Settings → Services → Databases.

Click + Add New Service.

Choose ClickHouse as the service type.

Fill in the Connection Details:
* Service Name: clickhouse (or whatever you want)
* Host and Port: clickhouse-server:8123
* Username: dbt_user
* Password: dbt_password
* Database / Schema: Leave empty.
* Https / Secure: Leave them off.

Click Test Connection. If successful, click Next and Save the service.

Metadata Agent Deployment: Once the Metadata Agent has successfully run, you should be able to see the tables and columns in OpenMetadata.

### **Example Screenshots**

| Data Quality Tests                                                                                                      | Column Descriptions                                                                                                     |
| ----------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| <img width="1587" height="308" src="https://github.com/user-attachments/assets/9f549e98-ba38-4e18-90c9-6ee95dee5070" /> | <img width="1199" height="558" alt="Screenshot 2025-11-30 at 12 46 02" src="https://github.com/user-attachments/assets/def3eeaa-697c-4d26-9226-a8920ef320e5" />
| <img width="1588" height="287" src="https://github.com/user-attachments/assets/fbf130e7-295b-4455-b473-aa431db848c0" /> |<img width="1333" height="562" alt="Screenshot 2025-11-30 at 12 10 55" src="https://github.com/user-attachments/assets/bac171de-5dde-44ad-9f33-85d34aaa9de9" />


---

### 11. Apache Superset Integration**

Apache Superset is used to create the BI dashboard for analyzing ratings metrics.

### **Accessing Superset**

Open in your browser:

```
http://localhost:8088
```

**Login credentials:**

```
Username: admin
Password: admin
```

Inside Superset:

* Go to **Data → Databases** to verify ClickHouse connection
* Go to **Dashboards** to open the movie ratings dashboard
* Use the filter to slice results (e.g., by genre)

### **Example Screenshots**

| Dashboard Overview                                                                                                      | Filter Example                                                                                                         |
| ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| <img width="1643" height="404" src="https://github.com/user-attachments/assets/f9155b7e-34ab-45de-87af-578309a130a5" /> | <img width="831" height="440" src="https://github.com/user-attachments/assets/5758db85-fb2b-4e2c-a803-720e2f5c4137" /> |

