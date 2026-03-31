# 1. GCP project identifier (required)
variable "project" {
  description = "Project ID"
  default     = "your-gcp-project-id" # Replace with your GCP project id (match scripts / profiles.yml)
}

# 2. Physical region where resources will be provisioned
variable "region" {
  description = "Region"
  default     = "us-central1"
}

# 3. Data storage location (multi-region level, e.g. US, EU)
variable "location" {
  description = "Project Location"
  default     = "US"
}

# 4. BigQuery dataset name (analytics DB container)
variable "bq_dataset_name" {
  description = "BigQuery dataset name (e.g. GitHub Archive track)"
  default     = "github_archive_data"
}

# 5. GCS bucket name (file storage container)
# Note: Must be globally unique across all Google Cloud users
variable "gcs_bucket_name" {
  description = "GCS bucket name (globally unique)"
  default     = "your-gcs-bucket-name"
}

# 6. Storage class (STANDARD = fastest performance)
variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

# 7. Path to service account key for GCP authentication
variable "credentials" {
  description = "GCP Credentials path"
  default     = "./gcp-creds.json"
}
