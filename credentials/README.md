# `credentials/` — local secrets only (nothing here is committed by default)

Use this folder for **machine-local** files: GCP keys, UI passwords, and other secrets.  
**Do not rename this directory** to look like a file (e.g. `something.json` as a folder name).

## Google Cloud (required for GCP pipelines)

**`gcp-service-account.json`** — service account key JSON (gitignored).

1. [Google Cloud Console](https://console.cloud.google.com/) → IAM → Service accounts → Keys → Add key → JSON.
2. Save as **`gcp-service-account.json`** here (or set **`GCP_CREDS_PATH`** elsewhere).

## Local Docker UI logins (Kestra + Grafana)

When you run **`docker compose`**, you use **Kestra** (orchestration) and **Grafana** (streaming dashboards). It is easy to forget which email/password you set.

**Recommended:**

1. Copy **`local-dev-ui.env.example`** → **`local-dev-ui.env`** (same directory).
2. Edit **`local-dev-ui.env`** with the values that match **`docker-compose.yml`** (or your own if you changed them there).

**`local-dev-ui.env`** is **gitignored** — your real passwords stay on your machine.  
The **`.example`** file is safe to commit (defaults only).

> These env files are **not** automatically loaded by Compose in this repo; they are a **personal cheat sheet**. If you later wire `env_file:` in Compose, keep using a gitignored path.

## Files at a glance

| File | In Git? | Purpose |
|------|---------|---------|
| `gcp-service-account.json` | No (ignored) | GCP API authentication |
| `local-dev-ui.env` | No (ignored) | Your Kestra / Grafana reminders |
| `local-dev-ui.env.example` | Yes | Template + documented defaults |
