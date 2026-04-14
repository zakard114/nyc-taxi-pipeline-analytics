# Drop into repo flink-libs/ (see docker-compose Flink services). Restart Flink after adding JARs:
#   docker compose restart flink-jobmanager flink-taskmanager
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$dest = Join-Path $root "flink-libs"
New-Item -ItemType Directory -Force -Path $dest | Out-Null

$jars = @(
    @{
        Name = "flink-connector-jdbc-3.2.0-1.18.jar"
        Url  = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.18/flink-connector-jdbc-3.2.0-1.18.jar"
    },
    @{
        Name = "postgresql-42.7.4.jar"
        Url  = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
    }
)
foreach ($j in $jars) {
    $path = Join-Path $dest $j.Name
    if (Test-Path $path) {
        Write-Host "Skip (exists): $($j.Name)"
        continue
    }
    Write-Host "Downloading $($j.Name) ..."
    Invoke-WebRequest -Uri $j.Url -OutFile $path -UseBasicParsing
}
Write-Host "Done. Restart Flink: docker compose restart flink-jobmanager flink-taskmanager"
