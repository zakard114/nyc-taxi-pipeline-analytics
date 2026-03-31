# After `git clone` — local files not in Git

These paths are **gitignored** on purpose (secrets, large data, machine-local build output). Copy them from a secure backup machine or recreate them after cloning.

## Required for GCP / Terraform / ingest

| Item | Typical path | Notes |
|------|----------------|------|
| Service account key | `terraform/gcp-creds.json` | Same JSON you use for Terraform and `GCP_CREDS_PATH`; **never commit**. |
| Terraform variables | `terraform/terraform.tfvars` | Optional; overrides for `project_id`, bucket name, etc. |
| Environment | `.env` | If you use one; copy from `.env.example` pattern (if present). |

## dbt

| Item | Notes |
|------|------|
| `~/.dbt/profiles.yml` | Usually **outside** the repo; point `nyc_taxi_dbt` profile at your GCP project and dataset. |

Run `cd nyc_taxi_dbt && dbt deps` (if applicable), `dbt seed`, `dbt run` after BigQuery has source tables.

## Optional (large / reproducible)

| Item | Notes |
|------|------|
| `data/**/*.parquet`, `data/**/*.csv` | Re-download with `scripts/ingest_tlc_2019_2020.py` instead of copying TB over USB. |
| Python venv | Recreate with your usual workflow (`python -m venv .venv`, then install packages used by `scripts/` and dbt). |

## Kestra

Paste the same service account JSON into **Kestra KV** `GCP_CREDS` (see main README), or use Secret Manager in production.

## Quick verify

```bash
# From repo root
python scripts/ingest_tlc_2019_2020.py --help
cd nyc_taxi_dbt && dbt parse
```

If those run without import errors, paths and tooling are mostly aligned.
