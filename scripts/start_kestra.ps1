# Start Kestra via Docker Compose
# Run from Project folder: .\scripts\start_kestra.ps1

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

# 1. Check Docker
Write-Host "Checking Docker..." -ForegroundColor Cyan
try {
    $null = docker info 2>&1
} catch {
    Write-Host "Docker is not running. Please start Docker Desktop and retry." -ForegroundColor Red
    exit 1
}

# 2. Ensure gcp-creds exists for volume mount
if (-not (Test-Path "terraform\gcp-creds.json")) {
    Write-Host "WARNING: terraform\gcp-creds.json not found. Creating placeholder (replace with real key)." -ForegroundColor Yellow
    if (-not (Test-Path "terraform")) { New-Item -ItemType Directory -Path "terraform" -Force }
    '{}' | Out-File -FilePath "terraform\gcp-creds.json" -Encoding utf8
}

# 3. Run docker compose
Write-Host "Starting Kestra..." -ForegroundColor Cyan
docker compose up -d

Write-Host "`nKestra UI: http://localhost:8080" -ForegroundColor Green
