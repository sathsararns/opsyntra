param(
    [ValidateSet("setup", "train", "predict", "test")]
    [string]$Task = "train",
    [string]$Record,
    [string]$InputCsv,
    [string]$Output
)

$ErrorActionPreference = "Stop"
$opsyntraRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../..')).Path
$backendDirectory = Join-Path $opsyntraRoot "backend"
$linuxBackendResult = & wsl -d Ubuntu --exec wslpath -a ($backendDirectory -replace '\\', '/')
if ($LASTEXITCODE -ne 0 -or -not $linuxBackendResult) { throw "The Ubuntu WSL runtime is required. See backend/README.md." }
$linuxBackend = $linuxBackendResult.Trim()
$pythonBinary = ".venv-spark/bin/python"
if ($Task -eq "setup") {
    & wsl -d Ubuntu -u root --exec apt-get update -qq
    if ($LASTEXITCODE -ne 0) { throw "Ubuntu package index update failed." }
    & wsl -d Ubuntu -u root --exec apt-get install -y -qq openjdk-21-jre-headless python3-venv
    if ($LASTEXITCODE -ne 0) { throw "Ubuntu Java/Python installation failed." }
    & wsl -d Ubuntu --cd $linuxBackend --exec python3 -m pipelines.spark_environment
    if ($LASTEXITCODE -ne 0) { throw "Spark environment creation failed." }
    & wsl -d Ubuntu --cd $linuxBackend --exec $pythonBinary -m pip install -r requirements/spark.txt
    if ($LASTEXITCODE -ne 0) { throw "Spark dependency installation failed." }
    return
}
$available = & wsl -d Ubuntu --cd $linuxBackend --exec test -x $pythonBinary
if ($LASTEXITCODE -ne 0) { throw "Spark environment missing. Follow backend/README.md to install requirements/spark.txt in backend/.venv-spark." }

switch ($Task) {
    "train" {
        & wsl -d Ubuntu --cd $linuxBackend --exec $pythonBinary -m pipelines.spark_training_pipeline --skip-tracking
        if ($LASTEXITCODE -ne 0) { throw "Spark training failed. Review the error above." }
        # Log from Windows so MLflow's existing Windows artifact paths stay valid.
        Push-Location $backendDirectory
        try {
            python -m pipelines.spark_tracking
            if ($LASTEXITCODE -ne 0) { throw "Spark finished, but MLflow logging failed. Run python -m pipelines.spark_tracking from backend." }
        } finally { Pop-Location }
    }
    "predict" {
        $predictionArguments = @("-m", "pipelines.spark_inference_pipeline")
        if ($Record -and $InputCsv) { throw "Use Record or InputCsv, not both." }
        if ($Record) { $predictionArguments += @("--record", $Record) }
        elseif ($InputCsv) { $predictionArguments += @("--input", $InputCsv) }
        else { throw "Provide -Record examples/customer.json or -InputCsv examples/customers.csv." }
        if ($Output) { $predictionArguments += @("--output", $Output) }
        & wsl -d Ubuntu --cd $linuxBackend --exec $pythonBinary @predictionArguments
        if ($LASTEXITCODE -ne 0) { throw "Spark inference failed. Review the error above." }
    }
    "test" {
        & wsl -d Ubuntu --cd $linuxBackend --exec env OPSYNTRA_RUN_SPARK_TESTS=1 $pythonBinary -m unittest tests.integration.test_spark_workflow -v
        if ($LASTEXITCODE -ne 0) { throw "Spark checks failed. Review the error above." }
    }
}
