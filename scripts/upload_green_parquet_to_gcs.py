"""
Upload a local TLC Green Taxi Parquet file to the path expected by Kestra flow
gcs_to_bigquery_green.

  python scripts/upload_green_parquet_to_gcs.py \\
    "C:\\path\\to\\green_tripdata_2021-01.parquet"

Requires: pip install google-cloud-storage
Run from repo root (so ./credentials/gcp-service-account.json resolves)
"""

import os
import sys
from pathlib import Path

from google.cloud import storage

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CREDS = Path(
    os.environ.get("GCP_CREDS_PATH", str(PROJECT_ROOT / "credentials" / "gcp-service-account.json"))
)
BUCKET = os.environ.get("GCS_BUCKET", "YOUR_GCS_BUCKET")
GCS_OBJECT = "raw/nyc_taxi/green_tripdata_2021-01.parquet"


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "Usage: python scripts/upload_green_parquet_to_gcs.py <local_parquet_file>"
        )
        sys.exit(1)
    local = Path(sys.argv[1]).expanduser().resolve()
    if not local.is_file():
        print(f"File not found: {local}")
        sys.exit(1)

    if not CREDS.is_file():
        print(f"Missing credentials: {CREDS}")
        sys.exit(1)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(CREDS)

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(GCS_OBJECT)
    blob.upload_from_filename(str(local))
    print(f"OK: gs://{BUCKET}/{GCS_OBJECT}")


if __name__ == "__main__":
    main()
