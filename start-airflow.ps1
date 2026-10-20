param(
    [ValidateSet("setup", "start", "stop", "run", "status", "imports", "record", "track", "run-kafka")]
    [string]$Task = "start",
    [ValidateSet("sklearn", "spark")]
    [string]$Engine = "sklearn",
    [string]$PredictionInput = "examples/customers.csv",
    [ValidateSet("batch", "streaming")]
    [string]$KafkaKind = "batch",
    [ValidateRange(5, 3600)]
    [int]$DurationSeconds = 60,
    [ValidateRange(0, 10000)]
    [int]$DemoEvents = 0,
    [double]$EventsPerSec = 5
)

$ErrorActionPreference = "Stop"
$backendDirectory = Join-Path $PSScriptRoot "backend"
$linuxBackendResult = & wsl -d Ubuntu --exec wslpath -a ($backendDirectory -replace '\\', '/')
if ($LASTEXITCODE -ne 0 -or -not $linuxBackendResult) { throw "The existing Ubuntu WSL distribution is required." }
$linuxBackend = $linuxBackendResult.Trim()
# Resolve the actual Ubuntu account's home, rather than assuming its user name.
$linuxHome = (& wsl -d Ubuntu --exec python3 -c "from pathlib import Path; print(Path.home())").Trim()
$venvPython = "$linuxHome/.local/share/opsyntra/airflow-venv/bin/python"
if ($Task -eq "track") {
    Push-Location $backendDirectory
    try {
        python -m pipelines.airflow_tracking --engine $Engine
        if ($LASTEXITCODE -ne 0) { throw "Airflow completed but MLflow logging failed." }
    } finally { Pop-Location }
    return
}
if ($Task -eq "setup") {
    & wsl -d Ubuntu --cd $linuxBackend --exec python3 -m venv "$linuxHome/.local/share/opsyntra/airflow-venv"
    if ($LASTEXITCODE -ne 0) { throw "Install python3-venv in Ubuntu first." }
    & wsl -d Ubuntu --cd $linuxBackend --exec $venvPython -m pip install -r requirements/airflow.txt
    if ($LASTEXITCODE -ne 0) { throw "Airflow installation failed." }
    & wsl -d Ubuntu --cd $linuxBackend --exec .venv-spark/bin/python -m pip install -r requirements/worker.txt
    if ($LASTEXITCODE -ne 0) { throw "ML worker setup failed. Run start-spark.ps1 -Task setup first, then retry Airflow setup." }
    return
}
& wsl -d Ubuntu --cd $linuxBackend --exec $venvPython -m pipelines.airflow_service $Task --engine $Engine --prediction-input $PredictionInput --kafka-kind $KafkaKind --duration-seconds $DurationSeconds --demo-events $DemoEvents --events-per-sec $EventsPerSec
if ($LASTEXITCODE -ne 0) { throw "Airflow $Task failed. Inspect the log output above." }
if ($Task -eq "run") {
    Push-Location $backendDirectory
    try {
        python -m pipelines.airflow_tracking --engine $Engine
        if ($LASTEXITCODE -ne 0) { throw "Airflow completed but MLflow logging failed. Retry with -Task track." }
    } finally { Pop-Location }
}
