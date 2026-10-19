$ErrorActionPreference = 'Stop'
$opsyntraRoot = $PSScriptRoot
$opsyntraBackend = Join-Path $opsyntraRoot 'backend'
$apiProcess = $null
$pythonExecutable = (Get-Command python -ErrorAction Stop).Source

if (-not (Test-Path -LiteralPath (Join-Path $opsyntraRoot 'frontend/node_modules/next/package.json'))) {
    throw 'Install the frontend dependencies first: cd frontend; npm ci'
}

Push-Location $opsyntraBackend
try {
    & $pythonExecutable -m pipelines.frontend_snapshot
    if ($LASTEXITCODE -ne 0) { throw 'Prepare the backend data/model artifacts and install backend/requirements/web.txt first.' }

    $apiReady = $false
    try {
        $health = Invoke-RestMethod -Uri 'http://127.0.0.1:8000/api/health' -TimeoutSec 2
        $apiReady = $health.status -eq 'ready' -and [bool]$health.model_version
    } catch { $apiReady = $false }

    if (-not $apiReady) {
        $logDirectory = Join-Path $opsyntraBackend 'logs'
        New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
        $apiProcess = Start-Process -FilePath $pythonExecutable -ArgumentList @(
            '-m', 'uvicorn', 'opsyntra.web_api:app', '--app-dir', 'src',
            '--host', '127.0.0.1', '--port', '8000'
        ) -WorkingDirectory $opsyntraBackend -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $logDirectory 'api.stdout.log') `
            -RedirectStandardError (Join-Path $logDirectory 'api.stderr.log')
        for ($attempt = 0; $attempt -lt 15; $attempt++) {
            if ($apiProcess.HasExited) { throw 'The API could not start. Check backend/logs/api.stderr.log and whether port 8000 is already in use.' }
            try {
                $health = Invoke-RestMethod -Uri 'http://127.0.0.1:8000/api/health' -TimeoutSec 2
                if ($health.status -eq 'ready') { $apiReady = $true; break }
            } catch { Start-Sleep -Milliseconds 700 }
        }
        if (-not $apiReady) { throw 'The API did not become ready. Check backend/logs/api.stderr.log.' }
    }

    Write-Host 'Opsyntra is ready at http://127.0.0.1:3000' -ForegroundColor Green
    Write-Host 'Press Ctrl+C to stop. An API started by this script is stopped on exit.'
    Set-Location (Join-Path $opsyntraRoot 'frontend')
    & npm run dev -- --port 3000
} finally {
    if ($null -ne $apiProcess -and -not $apiProcess.HasExited) {
        Stop-Process -Id $apiProcess.Id -ErrorAction SilentlyContinue
    }
    Pop-Location
}
