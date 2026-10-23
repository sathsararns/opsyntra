$ErrorActionPreference = "Stop"
$opsyntraRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../..')).Path
Push-Location (Join-Path $opsyntraRoot "backend")
try {
    python -m pipelines.mlflow_server @args
    if ($LASTEXITCODE -ne 0) { throw "MLflow exited with code $LASTEXITCODE" }
} finally {
    Pop-Location
}
