"""
Quick check of loaded GitHub events in DuckDB.
Uses fetchall() to avoid pandas/numpy dependency.
"""
import duckdb  # DuckDB library for in-process SQL analytics

# Connect to the local DuckDB database file
conn = duckdb.connect("github_test.duckdb")

# Print total row count from github_events table in github_raw schema
print("Count:", conn.sql("SELECT COUNT(*) AS cnt FROM github_raw.github_events").fetchone())

# Print header for sample rows
print("\nSample (id, type, created_at):")

# Fetch first 5 rows and print each (id, type, created_at)
for row in conn.sql("SELECT id, type, created_at FROM github_raw.github_events LIMIT 5").fetchall():
    print(row)

# Close the database connection to release resources
conn.close()
