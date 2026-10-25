param(
    [ValidateSet('setup', 'start', 'stop', 'status', 'logs', 'demo', 'test', 'reset')]
    [string]$Task = 'start',
    [int]$DemoEvents = 20,
    [ValidateRange(10, 600)]
    [int]$DockerStartupTimeoutSeconds = 180,
    [switch]$ConfirmReset
)
$ErrorActionPreference = 'Stop'
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker CLI was not found. Install Docker Desktop and reopen PowerShell before using this launcher.'
}
if ($Task -ne 'setup') {
    function Get-OpsyntraDockerEngine {
        try {
            $engineType = & docker info --format '{{.OSType}}' 2>$null
            if ($LASTEXITCODE -eq 0) { return ($engineType | Out-String).Trim() }
        } catch { }
        return $null
    }
    $dockerEngine = Get-OpsyntraDockerEngine
    if (-not $dockerEngine -and $Task -eq 'start') {
        $context = (& docker context show 2>$null | Out-String).Trim()
        $desktop = Join-Path $env:ProgramFiles 'Docker/Docker/Docker Desktop.exe'
        # Only start the local Desktop runtime; preserve explicitly configured remote contexts.
        if ($context -eq 'desktop-linux' -and -not $env:DOCKER_HOST -and -not $env:DOCKER_CONTEXT -and (Test-Path -LiteralPath $desktop)) {
            if (-not (Get-Process -Name 'Docker Desktop' -ErrorAction SilentlyContinue)) {
                Write-Host 'Starting Docker Desktop...' -ForegroundColor Cyan
                Start-Process -FilePath $desktop -WindowStyle Hidden
            }
            Write-Host "Waiting for the Docker Linux engine (up to $DockerStartupTimeoutSeconds seconds)..." -ForegroundColor Cyan
            $startupTimer = [Diagnostics.Stopwatch]::StartNew()
            $nextProgress = 15
            while (-not $dockerEngine -and $startupTimer.Elapsed.TotalSeconds -lt $DockerStartupTimeoutSeconds) {
                Start-Sleep -Seconds 2
                $dockerEngine = Get-OpsyntraDockerEngine
                if (-not $dockerEngine -and $startupTimer.Elapsed.TotalSeconds -ge $nextProgress) {
                    Write-Host 'Docker Desktop is still starting...'
                    $nextProgress += 15
                }
            }
        }
    }
    if (-not $dockerEngine) {
        throw 'Docker engine is unavailable. Open Docker Desktop and check its startup status. If Docker Desktop is open, inspect its error before retrying. For the local Linux engine, check docker context show (normally desktop-linux). No project data has been reset.'
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
