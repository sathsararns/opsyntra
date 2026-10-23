param(
    [ValidateSet('setup', 'start', 'stop', 'status', 'logs', 'demo', 'test', 'reset')]
    [string]$Task = 'start',
    [int]$DemoEvents = 20,
    [switch]$ConfirmReset
)
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    if ($Task -eq 'setup' -or $Task -eq 'start') {
        python containers/configure.py
        if ($LASTEXITCODE -ne 0) { throw 'Could not prepare root .env' }
    }
    if ($Task -eq 'test') {
        python containers/verify.py
    } elseif ($Task -eq 'setup') {
        docker compose config --quiet
    } elseif ($Task -eq 'start') {
        docker compose up --build -d --wait --wait-timeout 900
    } elseif ($Task -eq 'stop') {
        docker compose --profile '*' down
    } elseif ($Task -eq 'status') {
        docker compose --profile '*' ps --all
    } elseif ($Task -eq 'logs') {
        docker compose logs --follow --tail 100
    } elseif ($Task -eq 'demo') {
        if ($DemoEvents -lt 1) { throw 'DemoEvents must be positive' }
        docker compose run --rm --no-deps producer producer --mode streaming --events-per-sec 5 --max-events $DemoEvents --exclude-churn
    } elseif ($Task -eq 'reset') {
        if (-not $ConfirmReset) { throw 'Reset permanently deletes this Compose project data, models and recovery state. Use -ConfirmReset only when you intend that deletion.' }
        docker compose --profile '*' down --volumes
    }
    if ($LASTEXITCODE -ne 0) { throw "Container command failed with exit code $LASTEXITCODE" }
} finally {
    Pop-Location
}
