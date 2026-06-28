# NYC Taxi ELT Pipeline with dbt, DuckDB & Airflow

## Project Overview

This project implements an end-to-end ELT (Extract, Load, Transform) pipeline using the 2023 NYC Yellow Taxi dataset. The pipeline loads raw taxi trip data into DuckDB, transforms it using dbt, validates data quality through automated tests, and orchestrates the workflow using Apache Airflow.

The project demonstrates modern analytics engineering practices including data modeling, testing, orchestration, and containerization.

---

## Tech Stack

* Python 3.12
* DuckDB
* dbt Core
* dbt-duckdb
* Apache Airflow
* Docker & Docker Compose
* SQL
* Git & GitHub

---

## Project Structure

```
.
├── dags/                    # Airflow DAG
├── dbt/
│   └── nyc_taxi/            # dbt project
├── data/                    # Raw input datasets (ignored by Git)
├── queries/                 # SQL challenge solutions
├── spark/                   # Spark processing example
├── Dockerfile
├── docker-compose.yaml
├── load_raw_data.py
├── run_query.py
├── requirements.txt
└── README.md
```

---

## Architecture

```
NYC Taxi Parquet Files
          │
          ▼
   load_raw_data.py
          │
          ▼
      DuckDB Database
          │
          ▼
    dbt Staging Models
          │
          ▼
 Intermediate Models
          │
          ▼
      Mart Models
          │
          ▼
      Data Tests
          │
          ▼
 Apache Airflow Pipeline
```

---

## Data Pipeline

The Airflow DAG performs the following tasks:

1. Check source freshness
2. Execute dbt staging models
3. Execute intermediate models
4. Execute mart models
5. Execute dbt tests
6. Notify successful completion

---

## dbt Models

### Staging

* stg_yellow_trips
* stg_taxi_zones

### Intermediate

* int_trips_enriched

### Marts

* fct_trips
* dim_zones
* agg_daily_revenue
* agg_zone_performance

---

## Data Quality Tests

Implemented using dbt:

* Not Null tests
* Unique tests
* Custom SQL test
* Generic macro test

---

## SQL Challenge

Implemented solutions for:

* Top revenue generating pickup zones
* Hour-of-day trip pattern analysis
* Consecutive trip gap analysis

---

## Running the Project

### Install dependencies

```bash
pip install -r requirements.txt
```

### Load raw data

```bash
python load_raw_data.py
```

### Execute dbt models

```bash
cd dbt/nyc_taxi

dbt run
dbt test
```

### Start Airflow

```bash
docker compose up -d
```

Airflow UI:

```
http://localhost:8080
```

Username:

```
airflow
```

Password:

```
airflow
```

---

## Validation

The pipeline was successfully validated by:

* Running all dbt models
* Passing all dbt tests
* Successful Airflow DAG execution
* Verifying fact and dimension tables
* Executing analytical SQL queries

---

## Future Improvements

* Incremental dbt models
* CI/CD with GitHub Actions
* Great Expectations integration
* Cloud deployment (AWS/GCP/Azure)
* Data lineage visualization

---

## Author

Akshay Pratap Singh
