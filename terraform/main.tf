# 1. Terraform and provider configuration
terraform {
  required_providers {
    # Official Google Cloud provider for Terraform
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

# 2. Google Cloud provider connection settings
provider "google" {
  # Authenticate using service account key (JSON file)
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# 3. Google Cloud Storage (GCS) bucket creation (data lake)
resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true  # Allow bucket deletion even when it contains objects

  # Lifecycle rule: abort stalled multipart uploads after 1 day (cost savings)
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# 4. BigQuery dataset creation (data warehouse)
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}
