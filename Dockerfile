FROM apache/airflow:2.10.0

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.11.11 \
    dbt-duckdb==1.10.1 \
    duckdb \
    pandas \
    pyarrow