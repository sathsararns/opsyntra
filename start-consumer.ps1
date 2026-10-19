param(
    [ValidateSet("batch", "streaming")][string]$Mode = "batch",
    [ValidateSet("sklearn", "spark")][string]$Engine = "sklearn",
    [int]$MaxMessages = 0,
    [int]$BatchSize = 0,
    [string]$GroupId,
    [string]$OffsetWindow,
    [string]$Model,
    [string]$StateDir,
    [double]$IdleTimeout = 0,
    [double]$TimeoutSeconds = 0,
    [switch]$SetupSparkClient
)
$ErrorActionPreference = "Stop"
$backendDirectory = Join-Path $PSScriptRoot "backend"
function Resolve-BackendPath([string]$Value) {
    if ([IO.Path]::IsPathRooted($Value)) { return [IO.Path]::GetFullPath($Value) }
    return [IO.Path]::GetFullPath((Join-Path $backendDirectory $Value))
}
if ($MaxMessages -lt 0 -or $BatchSize -lt 0 -or $IdleTimeout -lt 0 -or $TimeoutSeconds -lt 0) { throw "Counts and timeouts must be positive." }
$consumerArguments = @("--mode", $Mode, "--engine", $Engine)
if ($MaxMessages) { $consumerArguments += @("--max-messages", "$MaxMessages") }
if ($BatchSize) { $consumerArguments += @("--batch-size", "$BatchSize") }
if ($GroupId) { $consumerArguments += @("--group-id", $GroupId) }
if ($IdleTimeout) { $consumerArguments += @("--idle-timeout", "$IdleTimeout") }
if ($TimeoutSeconds) { $consumerArguments += @("--timeout-seconds", "$TimeoutSeconds") }
if ($Engine -eq "sklearn" -and -not $SetupSparkClient) {
    # Paths on this wrapper are backend-relative, independent of terminal cwd.
    foreach ($entry in @(@("--offset-window", $OffsetWindow), @("--model", $Model), @("--state-dir", $StateDir))) {
        if ($entry[1]) { $consumerArguments += @($entry[0], (Resolve-BackendPath $entry[1])) }
    }
    & python (Join-Path $backendDirectory "consumer.py") @consumerArguments
    if ($LASTEXITCODE -ne 0) { throw "Consumer exited with code $LASTEXITCODE. See its summary and logs." }
    return
}
$linuxBackend = (& wsl -d Ubuntu --exec wslpath -a ($backendDirectory -replace '\\', '/')).Trim()
if ($LASTEXITCODE -ne 0) { throw "The existing Ubuntu WSL Spark environment is required." }
if ($SetupSparkClient) {
    & wsl -d Ubuntu --cd $linuxBackend --exec .venv-spark/bin/python -m pip install -r requirements/kafka.txt
    if ($LASTEXITCODE -ne 0) { throw "Spark Kafka client installation failed." }
    return
}
foreach ($entry in @(@("--offset-window", $OffsetWindow), @("--model", $Model), @("--state-dir", $StateDir))) {
    if ($entry[1]) {
        $absolute = Resolve-BackendPath $entry[1]
        $linuxPath = (& wsl -d Ubuntu --exec wslpath -a ($absolute -replace '\\', '/')).Trim()
        if ($LASTEXITCODE -ne 0) { throw "Unable to resolve $absolute inside WSL." }
        $consumerArguments += @($entry[0], $linuxPath)
    }
}
& wsl -d Ubuntu --cd $linuxBackend --exec .venv-spark/bin/python consumer.py @consumerArguments
if ($LASTEXITCODE -ne 0) { throw "Spark consumer exited with code $LASTEXITCODE. See its summary and logs." }
