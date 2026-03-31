# dlt: data load tool for ETL pipelines (DuckDB, BigQuery, etc.)
import dlt
# requests: HTTP library for fetching data from URLs
import requests
# gzip: decompress gzip-compressed response from GitHub Archive
import gzip
# json: parse each line of JSON from the archive
import json

# 1. Define the GitHub Archive data source as a dlt source
@dlt.source
def github_archive_source(date_str):
    """
    Generator that fetches data for a specific date/hour.
    Example: '2026-03-01-15' (March 1st, 2026, 3 PM)
    """
    # Build GitHub Archive URL (one JSON.gz file per hour)
    url = f"https://data.gharchive.org/{date_str}.json.gz"

    # Resource with append disposition: new records added without overwriting
    @dlt.resource(write_disposition="append")
    def github_events():
        print(f"Fetching data from: {url}")
        # Stream the response to avoid loading entire file into memory
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            # Decompress gzip and read line-by-line (each line = one JSON object)
            with gzip.GzipFile(fileobj=response.raw) as f:
                for line in f:
                    # Parse each line as JSON and yield to dlt pipeline
                    yield json.loads(line)
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")

    # Return the resource generator to dlt
    return github_events()

# 2. Pipeline execution (run when script is executed directly)
if __name__ == "__main__":
    # Create pipeline: DuckDB destination for local testing (no cloud cost)
    pipeline = dlt.pipeline(
        pipeline_name="github_test",   # Pipeline identifier for dlt state
        destination="duckdb",          # Local DuckDB file (github_test.duckdb)
        dataset_name="github_raw"      # Schema/dataset name in DuckDB
    )

    # Run pipeline: fetch 2026-03-01 15:00 UTC, limit to 100 rows to avoid large disk usage
    load_info = pipeline.run(github_archive_source("2026-03-01-15").add_limit(100))
    print(load_info)
