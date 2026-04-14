"""
Quick check of loaded GitHub events in DuckDB.
Uses fetchall() to avoid pandas/numpy dependency.
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "github" / "github_test.duckdb"

conn = duckdb.connect(str(DB_PATH))
print(
    "Count:",
    conn.sql("SELECT COUNT(*) AS cnt FROM github_raw.github_events").fetchone(),
)

print("\nSample (id, type, created_at):")
for row in conn.sql(
    "SELECT id, type, created_at FROM github_raw.github_events LIMIT 5"
).fetchall():
    print(row)

conn.close()
