# upload_to_gcp.py — GitHub Archive sample: GCS upload + BigQuery load
import os
from pathlib import Path

from google.cloud import bigquery, storage

# Repository root (parent of ``scripts/``)
REPO_ROOT = Path(__file__).resolve().parents[1]

# 1. Configuration (must match Terraform-provisioned infrastructure; override with env)
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "YOUR_GCP_PROJECT")
BUCKET_NAME = os.environ.get("GCS_BUCKET", "YOUR_GCS_BUCKET")
DATASET_ID = os.environ.get("BQ_DATASET_GITHUB", "github_archive_data")
TABLE_ID = os.environ.get("BQ_TABLE_GITHUB_EVENTS", "github_events_100")
_creds = os.environ.get("GCP_CREDS_PATH", "").strip()
CREDS_PATH = _creds if _creds else str(REPO_ROOT / "credentials" / "gcp-service-account.json")

LOCAL_FILE = REPO_ROOT / "data" / "github" / "github_events_100.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDS_PATH


def upload_to_gcs(local_path, bucket_name, gcs_path):
    """Upload a local file to a specific path in a GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(str(local_path))
    print(f"✅ GCS upload complete: gs://{bucket_name}/{gcs_path}")


def load_to_bigquery(gcs_uri, table_id):
    """Load data from GCS into a BigQuery table."""
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"✅ BigQuery load complete: {DATASET_ID}.{table_id}")


if __name__ == "__main__":
    GCS_FILE_PATH = "raw/github_events_100.json"

    if not LOCAL_FILE.is_file():
        raise SystemExit(
            f"Missing {LOCAL_FILE}\n"
            "  Run: python scripts/export_duckdb_to_json.py (from repo root) after dlt load."
        )

    upload_to_gcs(LOCAL_FILE, BUCKET_NAME, GCS_FILE_PATH)
    gcs_uri = f"gs://{BUCKET_NAME}/{GCS_FILE_PATH}"
    load_to_bigquery(gcs_uri, TABLE_ID)
