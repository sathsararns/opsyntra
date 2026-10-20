param(
    [ValidateSet('setup', 'start', 'stop', 'status', 'migrate', 'test', 'test-consumer')]
    [string]$Task = 'status',
    [string]$Python = 'python',
    [int]$Port = 5433
)
$ErrorActionPreference = 'Stop'
$Backend = Join-Path $PSScriptRoot 'backend'
function Assert-Success([string]$Action) {
    if ($LASTEXITCODE -ne 0) { throw "$Action failed (exit $LASTEXITCODE)" }
}
if ($Task -in @('setup', 'start', 'stop', 'status')) {
    if ($Task -eq 'setup') {
        & $Python -m pip install -r (Join-Path $Backend 'requirements/analytics.txt')
        Assert-Success 'Analytics dependency installation'
        & wsl -d Ubuntu -u root --exec test -d /usr/lib/postgresql
        if ($LASTEXITCODE -ne 0) {
            & wsl -d Ubuntu -u root --exec apt-get update -qq
            Assert-Success 'PostgreSQL package index'
            & wsl -d Ubuntu -u root --exec env DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql postgresql-client
            Assert-Success 'PostgreSQL installation'
        }
    }
    $NativeBackendResult = & wsl -d Ubuntu --exec wslpath -a ($Backend -replace '\\', '/')
    Assert-Success 'WSL path conversion'
    if (-not $NativeBackendResult) { throw 'WSL returned no backend path' }
    $NativeBackend = $NativeBackendResult.Trim()
    & wsl -d Ubuntu -u root --cd $NativeBackend --exec python3 -m pipelines.analytics_local $Task --port $Port
    Assert-Success 'Local analytics database'
}
if ($Task -in @('setup', 'migrate', 'test', 'test-consumer')) {
    Push-Location $Backend
    try {
        if ($Task -eq 'test-consumer') { & $Python -m pipelines.analytics_consumer_check --live }
        elseif ($Task -eq 'test') { & $Python -m pipelines.analytics_check }
        else { & $Python -m pipelines.analytics_db migrate }
        Assert-Success 'Analytics schema verification'
    } finally { Pop-Location }
}
