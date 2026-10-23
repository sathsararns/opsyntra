param(
    [ValidateSet('setup', 'start', 'stop', 'status', 'logs', 'demo', 'test', 'reset')]
    [string]$Task = 'start',
    [int]$DemoEvents = 20,
    [switch]$ConfirmReset
)
$ErrorActionPreference = 'Stop'
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker CLI was not found. Install Docker Desktop and reopen PowerShell before using this launcher.'
}
if ($Task -ne 'setup') {
    $dockerReady = $false
    try {
        $dockerEngine = & docker info --format '{{.OSType}}' 2>$null
        $dockerReady = $LASTEXITCODE -eq 0
    } catch {
        $dockerReady = $false
    }
    if (-not $dockerReady) {
        throw 'Docker engine is unavailable. Open Docker Desktop, wait until its engine is running, then rerun this command. Check Docker Desktop and the active Docker context if it is already open.'
    }
    if (($dockerEngine | Out-String).Trim() -ne 'linux') {
        throw 'Opsyntra requires Linux containers. Switch Docker Desktop to Linux containers, wait until the engine is running, then rerun this command.'
    }
}
$opsyntraRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../..')).Path
Push-Location $opsyntraRoot
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
