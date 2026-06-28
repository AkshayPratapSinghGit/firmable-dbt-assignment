from datetime import datetime, timedelta
import logging
import os

import duckdb

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ============================================================
# Project Configuration
# ============================================================

PROJECT_DIR = "/opt/project"

DATA_DIR = os.path.join(PROJECT_DIR, "data")

DBT_PROJECT = "/opt/project/dbt/nyc_taxi"

DBT_PROFILES = "/home/airflow/.dbt"

DUCKDB_PATH = "/opt/project/dbt/nyc_taxi/nyc_taxi.duckdb"


# ============================================================
# Default Arguments
# ============================================================

default_args = {
    "owner": "Akshay",
    "depends_on_past": False,

    "email": [
        os.getenv(
            "AIRFLOW_ALERT_EMAIL",
            "akshaypratapone@gmail.com"
        )
    ],

    "email_on_failure": True,

    "retries": 2,

    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# Python Tasks
# ============================================================

def check_source_file(**context):
    """
    Validate that the required source files exist.

    The assessment dataset is historical (2023), therefore the
    DAG validates that all expected source files are available
    before starting the ELT pipeline.

    This implementation is compatible with manual runs and
    Airflow backfills.
    """

    required_files = [
        "taxi_zone_lookup.csv",
    ]

    # Add all monthly parquet files
    for month in range(1, 13):
        required_files.append(
            f"yellow_tripdata_2023-{month:02d}.parquet"
        )

    missing_files = []

    for filename in required_files:

        full_path = os.path.join(DATA_DIR, filename)

        if not os.path.exists(full_path):
            missing_files.append(filename)

    if missing_files:

        raise FileNotFoundError(
            f"Missing source files:\n{missing_files}"
        )

    logging.info(
        "All %d required source files found.",
        len(required_files),
    )


def notify_success(**context):
    """
    Log pipeline execution summary.

    Assignment Requirement:
    Log trip count and revenue.
    """

    con = duckdb.connect(DUCKDB_PATH)

    trip_count = con.sql(
        """
        SELECT COUNT(*)
        FROM fct_trips
        """
    ).fetchone()[0]

    total_revenue = con.sql(
        """
        SELECT
            ROUND(SUM(total_amount),2)
        FROM fct_trips
        """
    ).fetchone()[0]

    execution_date = context["ds"]

    logging.info("=" * 60)
    logging.info("NYC Taxi Pipeline completed successfully")
    logging.info("=" * 60)

    logging.info(
        "Execution Date : %s",
        execution_date,
    )

    logging.info(
        "Trip Count     : %s",
        f"{trip_count:,}",
    )

    logging.info(
        "Revenue        : $%.2f",
        total_revenue,
    )

    logging.info("=" * 60)

    con.close()


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id="nyc_taxi_daily_pipeline",

    description="NYC Taxi ELT Pipeline using DuckDB + dbt + Airflow",

    start_date=datetime(2024, 1, 1),

    schedule="0 2 * * *",

    catchup=False,

    default_args=default_args,

    tags=[
        "dbt",
        "duckdb",
        "nyc_taxi",
        "elt",
    ],
) as dag:
    
    # ============================================================
    # Check Source Freshness
    # ============================================================

    check_source_freshness = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_file,
    )

    # ============================================================
    # dbt Staging Models
    # ============================================================

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"""
        cd {DBT_PROJECT}

        /home/airflow/.local/bin/dbt run \
            --profiles-dir {DBT_PROFILES} \
            --select staging
        """,
    )

    # ============================================================
    # dbt Intermediate Models
    # ============================================================

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=f"""
        cd {DBT_PROJECT}

        /home/airflow/.local/bin/dbt run \
            --profiles-dir {DBT_PROFILES} \
            --select intermediate
        """,
    )

    # ============================================================
    # dbt Mart Models
    # ============================================================

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"""
        cd {DBT_PROJECT}

        /home/airflow/.local/bin/dbt run \
            --profiles-dir {DBT_PROFILES} \
            --select marts
        """,
    )

    # ============================================================
    # dbt Data Quality Tests
    # ============================================================

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"""
        cd {DBT_PROJECT}

        /home/airflow/.local/bin/dbt test \
            --profiles-dir {DBT_PROFILES}
        """,
    )

    # ============================================================
    # Success Notification
    # ============================================================

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # ============================================================
    # Pipeline Dependencies
    # ============================================================

    (
        check_source_freshness
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_marts
        >> run_dbt_tests
        >> notify
    )