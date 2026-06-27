import duckdb
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent

DB_PATH = PROJECT_ROOT / "dbt" / "nyc_taxi" / "nyc_taxi.duckdb"

PARQUET_FILE = PROJECT_ROOT / "data" / "yellow_tripdata_2023-01.parquet"

ZONE_FILE = PROJECT_ROOT / "data" / "taxi_zone_lookup.csv"

con = duckdb.connect(str(DB_PATH))

con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

print("Loading Yellow Taxi Trips...")

con.execute(f"""
CREATE OR REPLACE TABLE raw.yellow_tripdata AS
SELECT *
FROM read_parquet('{PARQUET_FILE}');
""")

print("Loading Taxi Zones...")

con.execute(f"""
CREATE OR REPLACE TABLE raw.taxi_zone_lookup AS
SELECT *
FROM read_csv_auto('{ZONE_FILE}');
""")

print("Done!")

con.close()