# upload_to_gcp.py
import os  # OS utilities (file paths, environment variables)
from google.cloud import storage  # Google Cloud Storage (GCS) client
from google.cloud import bigquery  # Google BigQuery client
import pandas as pd  # Data manipulation (used internally)

# 1. Configuration (must match Terraform-provisioned infrastructure; override with env for public clones)
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "YOUR_GCP_PROJECT")
BUCKET_NAME = os.environ.get("GCS_BUCKET", "YOUR_GCS_BUCKET")
DATASET_ID = os.environ.get("BQ_DATASET_GITHUB", "github_archive_data")
TABLE_ID = os.environ.get("BQ_TABLE_GITHUB_EVENTS", "github_events_100")
CREDS_PATH = os.environ.get("GCP_CREDS_PATH", "./terraform/gcp-creds.json")

# Set credentials path for GCP API authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDS_PATH


def upload_to_gcs(local_path, bucket_name, gcs_path):
    """Upload a local file to a specific path in a GCS bucket."""
    client = storage.Client()           # Create GCS client
    bucket = client.bucket(bucket_name)  # Select target bucket
    blob = bucket.blob(gcs_path)         # Define blob (file path) inside bucket
    blob.upload_from_filename(local_path)  # Upload local file to GCS
    print(f"✅ GCS upload complete: gs://{bucket_name}/{gcs_path}")


def load_to_bigquery(gcs_uri, table_id):
    """Load data from GCS into a BigQuery table."""
    client = bigquery.Client()  # Create BigQuery client
    table_ref = client.dataset(DATASET_ID).table(table_id)  # Target table reference

    # Load job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,  # NDJSON format
        autodetect=True,  # Auto-detect schema from data
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table on each run
    )

    # Start load job from GCS URI into BigQuery
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for async job to complete
    print(f"✅ BigQuery load complete: {DATASET_ID}.{table_id}")


if __name__ == "__main__":
    # Step 1: Define local file path and GCS destination path
    LOCAL_FILE = "github_events_100.json"   # 100-sample data file from step 2
    GCS_FILE_PATH = "raw/github_events_100.json"  # Store under raw/ folder in bucket

    # Step 2: Upload file to GCS
    upload_to_gcs(LOCAL_FILE, BUCKET_NAME, GCS_FILE_PATH)

    # Step 3: Build full GCS URI (gs://bucket_name/path)
    gcs_uri = f"gs://{BUCKET_NAME}/{GCS_FILE_PATH}"

    # Step 4: Load GCS data into BigQuery table
    load_to_bigquery(gcs_uri, TABLE_ID)
