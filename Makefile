# NYC taxi pipeline — common commands (GNU Make: Git Bash / WSL / macOS / Linux).
# On Windows without `make`, run the shell commands from each target by hand.

.PHONY: help dbt-parse dbt-seed dbt-run dbt-test dbt-all terraform-init terraform-plan docker-up docker-down docker-config pytest lint readme-anchors

DBT_DIR := nyc_taxi_dbt
TF_DIR := terraform

help:
	@echo "Targets:"
	@echo "  make dbt-parse     - dbt parse (project syntax; needs ~/.dbt/profiles.yml)"
	@echo "  make dbt-seed      - dbt seed"
	@echo "  make dbt-run       - dbt run"
	@echo "  make dbt-test      - dbt test"
	@echo "  make dbt-all       - dbt seed && dbt run && dbt test"
	@echo "  make terraform-init / terraform-plan"
	@echo "  make docker-up / docker-down / docker-config"
	@echo "  make pytest        - unit tests (ingest helpers; no GCP)"
	@echo "  make lint          - ruff check scripts, tests, dlt"
	@echo "  make readme-anchors - verify README TOC #anchors match headings"

dbt-parse:
	cd $(DBT_DIR) && dbt parse

dbt-seed:
	cd $(DBT_DIR) && dbt seed

dbt-run:
	cd $(DBT_DIR) && dbt run

dbt-test:
	cd $(DBT_DIR) && dbt test

dbt-all: dbt-seed dbt-run dbt-test

terraform-init:
	cd $(TF_DIR) && terraform init

terraform-plan:
	cd $(TF_DIR) && terraform plan

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-config:
	docker compose config

pytest:
	python -m pytest tests -q

lint:
	python -m ruff check scripts tests dlt

readme-anchors:
	python scripts/verify_readme_anchors.py
