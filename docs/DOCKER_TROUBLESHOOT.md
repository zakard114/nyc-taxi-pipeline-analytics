# Docker Troubleshooting (Internal Server Error)

If `docker compose up -d` fails with **Internal Server Error for API route**, fix Docker first:

## Quick Fixes (try in order)

### 1. Restart Docker Desktop
- Right-click Docker icon in system tray → **Restart**
- Or: Quit Docker Desktop completely, then start again

### 2. Reset WSL2 (if using WSL2 backend)
```powershell
wsl --shutdown
```
Then restart Docker Desktop.

### 3. Check Docker status
```bash
docker info
```
If this hangs or errors, Docker daemon is not healthy.

### 4. Switch Docker context
```bash
docker context use default
```

### 5. Docker Desktop Settings
- **General** → Enable "Use the WSL 2 based engine"
- **Resources** → Ensure enough memory/disk

## After Docker works

From `Project/` folder:
```bash
docker compose up -d
```

Or use PowerShell script:
```powershell
.\scripts\start_kestra.ps1
```
