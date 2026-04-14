# Optional helper: start local Kestra stack (see README — Docker Compose).
# Run from repository root.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# 1. Repo root = parent of scripts/
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

# 2. Ensure service account exists for volume mount (docker-compose)
$credPath = Join-Path $RepoRoot "credentials\gcp-service-account.json"
if (-not (Test-Path $credPath)) {
    Write-Host "WARNING: $credPath not found. Create it (see credentials\README.md) or Docker will fail to mount." -ForegroundColor Yellow
}

docker compose build kestra
docker compose up -d

Write-Host "Kestra UI: http://localhost:8090 (see docker-compose.yml for credentials)." -ForegroundColor Green
