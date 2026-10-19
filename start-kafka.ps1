param(
    [ValidateSet("setup", "start", "stop", "status", "topics", "verify")]
    [string]$Task = "start"
)
$ErrorActionPreference = "Stop"
$backendDirectory = Join-Path $PSScriptRoot "backend"
if ($Task -eq "setup") {
    & python -m pip install -r (Join-Path $backendDirectory "requirements/kafka.txt")
    if ($LASTEXITCODE -ne 0) { throw "Kafka Python dependency setup failed." }
}
if ($Task -in @("topics", "verify")) {
    Push-Location $backendDirectory
    try {
        if ($Task -eq "topics") { & python -m pipelines.kafka_topics }
        else { & python -m pipelines.kafka_publishing_check }
        if ($LASTEXITCODE -ne 0) { throw "Kafka $Task failed; inspect the output above." }
    } finally { Pop-Location }
    return
}
$linuxBackendResult = & wsl -d Ubuntu --exec wslpath -a ($backendDirectory -replace '\\', '/')
if ($LASTEXITCODE -ne 0 -or -not $linuxBackendResult) { throw "Ubuntu WSL is required for the local Kafka broker." }
& wsl -d Ubuntu --cd $linuxBackendResult.Trim() --exec python3 -m pipelines.kafka_service $Task
if ($LASTEXITCODE -ne 0) { throw "Kafka $Task failed; inspect the output above." }
if ($Task -eq "start") {
    Push-Location $backendDirectory
    try {
        & python -m pipelines.kafka_topics
        if ($LASTEXITCODE -ne 0) { throw "Broker started but topic setup failed. Retry -Task topics." }
    } finally { Pop-Location }
}
