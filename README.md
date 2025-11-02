# Project 2 — Automated Movie Data Warehouse (Airflow + dbt + ClickHouse)

## Overview

This project builds on **Project 1**, automating the entire movie analytics data pipeline.  
It uses **Apache Airflow** to orchestrate data ingestion and **dbt** to transform datasets into a **ClickHouse** star schema.  
The pipeline produces analytics-ready tables in the `_gold` schema for querying and reporting.

---

## Business Goal

To create a fully automated, reproducible movie analytics workflow that:

* Downloads and ingests IMDb and TMDb data
* Cleans and transforms it through bronze → silver → gold layers
* Builds a star schema for analytical queries
* Enables repeatable pipeline runs using Airflow

---

## Datasets

1. **TMDb Movies Dataset (Kaggle)** — movie metadata (title, release date, budget, revenue, etc.)  
2. **IMDb Datasets** — core movie IDs, ratings, votes, crew, and genres.

---

## Tools

* **Docker Compose** — containerized setup  
* **Apache Airflow** — orchestrates pipeline tasks  
* **dbt (Data Build Tool)** — transformations and data tests  
* **ClickHouse** — columnar data warehouse  
* **Kaggle API** — automated dataset download  

---

## Setup and Environment

### 1. Create a `.env` file in the project root

Example content:

```bash
# .env
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_api_key
AIRFLOW_UID=50000


2. Start Docker Environment

Run the following command from the project root:

docker compose up -d


Once all containers start, open Airflow at:

http://localhost:8080


Default credentials:

Username: airflow
Password: airflow


Wait until all containers are healthy:

clickhouse-server

airflow-webserver

airflow-scheduler

dbt

3. Running the Pipeline

Open the Airflow UI.

Enable and trigger the DAG named movie_pipeline.

Wait until all tasks complete successfully.

The pipeline performs the following:

Downloads IMDb and TMDb datasets via Kaggle API

Loads raw data into ClickHouse (bronze layer)

Transforms it into clean, structured data (silver layer)

Builds the final star schema (gold layer) under _gold

Runs dbt tests for data quality validation