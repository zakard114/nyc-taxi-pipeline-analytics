"""
Export 100 GitHub events from DuckDB to NDJSON (newline-delimited JSON).
Run before ``scripts/upload_to_gcp.py`` — writes ``data/github/github_events_100.json``.
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "github" / "github_test.duckdb"
OUTPUT_FILE = ROOT / "data" / "github" / "github_events_100.json"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

conn = duckdb.connect(str(DB_PATH))
conn.execute(
    f"COPY (SELECT * FROM github_raw.github_events LIMIT 100) TO '{OUTPUT_FILE.as_posix()}'"
)
conn.close()
print(f"Exported to {OUTPUT_FILE}")
