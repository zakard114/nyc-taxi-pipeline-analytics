"""
Export 100 GitHub events from DuckDB to NDJSON (newline-delimited JSON).
Run this before upload_to_gcp.py - creates github_events_100.json.
"""
import duckdb

DB_PATH = "github_test.duckdb"
OUTPUT_FILE = "github_events_100.json"

conn = duckdb.connect(DB_PATH)
# DuckDB COPY exports to NDJSON by default for .json files
conn.execute(
    f"COPY (SELECT * FROM github_raw.github_events LIMIT 100) TO '{OUTPUT_FILE}'"
)
conn.close()
print(f"Exported to {OUTPUT_FILE}")
