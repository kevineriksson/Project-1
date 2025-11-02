# Project 1 — Movie Data Architecture & Modeling

## Overview

This project explores how to design a data architecture and star schema for analyzing movie performance using large public datasets from IMDb and TMDb. It links business questions to a data model that supports analytics and reporting.

---

## Business Goal

To analyze movie trends, ratings, and revenues for a streaming service. The analysis focuses on:

* Average ratings by genre, director, and time
* Box office revenue trends
* Correlation between ratings and revenue
* Top movies by genre and year
* Effect of runtime on revenue

---

## Datasets

1. **TMDb Movies Dataset (Kaggle)**
   Contains metadata such as title, release date, revenue, budget, genres, and popularity.

2. **IMDb Non-Commercial Datasets**
   Includes official movie identifiers, ratings, votes, and crew details.

---

## Tools

* **Docker** – for environment setup
* **PostgreSQL** – to store and query data
* **Python** – for ingestion and transformation
* **pgAdmin** – for querying and visualization

---

## Data Architecture

**Flow:** IMDb + TMDb → Ingestion (CSV/TSV load) → Postgres Staging → Star Schema → SQL Queries & Reports

**Ingestion:** Manual CSV/TSV load (batch mode)
**Frequency:** Daily in real-world, single load for project
**Quality Checks:**

* Ensure no NULL movie IDs
* IMDb ID unique per movie
* Ratings between 0 and 10

---

## Data Model (Star Schema)

**Fact Table:** `fact_movie` — one row per movie release
**Dimensions:**

* `dim_movie` (Type 1) – movie info, no history tracking
* `dim_production` (Type 2) – track company name changes
* `dim_date` (Static) – release date info
* `dim_genre` (Static) – list of genres
