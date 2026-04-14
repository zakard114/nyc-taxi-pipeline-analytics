# After `git clone` — local files not in Git

These paths are **gitignored** on purpose (secrets, large data, machine-local build output). Copy them from a secure backup machine or recreate them after cloning.

## Required for GCP / Terraform / ingest

| Item | Typical path | Notes |
|------|----------------|------|
| Service account key | `credentials/gcp-service-account.json` | Same JSON you use for Terraform and `GCP_CREDS_PATH`; **never commit**. See **`credentials/README.md`**. |
| Kestra / Grafana reminders | `credentials/local-dev-ui.env` | Copy from **`credentials/local-dev-ui.env.example`**; **gitignored**. Optional cheat sheet for local UI URLs/passwords. |
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
| `data/github/github_test.duckdb`, `data/github/github_events_100.json` | Regenerate the GitHub Archive sample: run `dlt/github_archive_ingestion.py` then `scripts/export_duckdb_to_json.py` (see repo **`data/github/README.md`**). |
| Python venv | Recreate with your usual workflow (`python -m venv .venv`, then install packages used by `scripts/` and dbt). |

## Kestra

1. **Service account JSON** — Paste the same file you use locally into **Kestra KV** → key **`GCP_CREDS`** (namespace **`system`**). Never commit the file. See the main README table for **`GCP_BUCKET`** as well.
2. **Run the stack** — From the repo root: `docker compose build kestra` (first time or after `Dockerfile.kestra` changes), then `docker compose up -d`. UI: [http://localhost:8090](http://localhost:8090) (host port mapped in `docker-compose.yml`; avoids clashes with other apps on 8080); credentials are in `docker-compose.yml`.
3. **Flow `nyc_taxi_ingest_pipeline`** — After clone, edit **`kestra/flows/batch/nyc_taxi_ingest_pipeline.yaml`** → **`variables.workspace_host`** to your machine’s **absolute path** to this repository (Docker Desktop: forward slashes, e.g. `E:/path/to/nyc-taxi-pipeline-analytics`). Then import or sync the flow in the UI.
4. **Production** — Prefer **Secret Manager** or your platform’s secret store instead of KV for long-lived keys.

For KV over HTTP, see the main README (**REST API** note under Kestra KV): `text/plain` bodies and hyphenated bucket names as JSON strings.

## Quick verify

```bash
# From repo root
python scripts/ingest_tlc_2019_2020.py --help
cd nyc_taxi_dbt && dbt parse
```

If those run without import errors, paths and tooling are mostly aligned.
