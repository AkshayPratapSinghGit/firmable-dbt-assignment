from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# --------------------------------------------------------------------
# Project paths
# --------------------------------------------------------------------
PROJECT_DIR = "/opt/project"
DBT_PROJECT = "/opt/project/dbt/nyc_taxi"
DBT_PROFILES = "/home/airflow/.dbt"


# --------------------------------------------------------------------
# Python Tasks
# --------------------------------------------------------------------
def check_source_file():
    data_dir = os.path.join(PROJECT_DIR, "data")

    parquet_files = [
        f for f in os.listdir(data_dir)
        if f.endswith(".parquet")
    ]

    if not parquet_files:
        raise Exception("No parquet files found in data directory.")

    print(f"Found {len(parquet_files)} parquet files.")


def notify_success():
    print("=" * 60)
    print("NYC Taxi Pipeline completed successfully!")
    print("✓ Source validated")
    print("✓ Staging models built")
    print("✓ Intermediate models built")
    print("✓ Mart models built")
    print("✓ All dbt tests passed")
    print("=" * 60)


# --------------------------------------------------------------------
# Default DAG Arguments
# --------------------------------------------------------------------
default_args = {
    "owner": "Akshay",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# --------------------------------------------------------------------
# DAG Definition
# --------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    description="NYC Taxi ELT Pipeline using DuckDB + dbt",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["dbt", "duckdb", "nyc_taxi"],
) as dag:

    # --------------------------------------------------------------
    # Check source files
    # --------------------------------------------------------------
    check_source_freshness = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_file,
    )

    # --------------------------------------------------------------
    # Build staging models
    # --------------------------------------------------------------
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"""
        cd {DBT_PROJECT} &&

        /home/airflow/.local/bin/dbt run \
            --profiles-dir {DBT_PROFILES} \
            --select staging
        """,
    )

    # --------------------------------------------------------------
    # Build intermediate models
    # --------------------------------------------------------------
    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=f"""
        cd {DBT_PROJECT} &&

        /home/airflow/.local/bin/dbt run \
            --profiles-dir {DBT_PROFILES} \
            --select intermediate
        """,
    )

    # --------------------------------------------------------------
    # Build marts
    # --------------------------------------------------------------
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"""
        cd {DBT_PROJECT} &&

        /home/airflow/.local/bin/dbt run \
            --profiles-dir {DBT_PROFILES} \
            --select marts
        """,
    )

    # --------------------------------------------------------------
    # Run dbt Tests
    # --------------------------------------------------------------
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"""
        cd {DBT_PROJECT} &&

        /home/airflow/.local/bin/dbt test \
            --profiles-dir {DBT_PROFILES}
        """,
    )

    # --------------------------------------------------------------
    # Success Notification
    # --------------------------------------------------------------
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # --------------------------------------------------------------
    # Pipeline Order
    # --------------------------------------------------------------
    (
        check_source_freshness
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_marts
        >> run_dbt_tests
        >> notify
    )