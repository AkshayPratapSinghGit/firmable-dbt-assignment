import sys
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / "dbt" / "nyc_taxi" / "nyc_taxi.duckdb"

if len(sys.argv) != 2:
    print("Usage:")
    print("python run_query.py queries/q1_top_zones_by_revenue.sql")
    sys.exit(1)

sql_file = PROJECT_ROOT / sys.argv[1]

con = duckdb.connect(DB_PATH)

query = sql_file.read_text()

result = con.sql(query)

print(result.df())

con.close()