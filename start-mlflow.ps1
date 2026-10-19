$ErrorActionPreference = "Stop"
Push-Location (Join-Path $PSScriptRoot "backend")
try {
    python -m pipelines.mlflow_server @args
    if ($LASTEXITCODE -ne 0) { throw "MLflow exited with code $LASTEXITCODE" }
} finally {
    Pop-Location
}
