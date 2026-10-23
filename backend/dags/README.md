# Opsyntra Airflow ML workflow

The two additional bonus DAGs are defined in `opsyntra_kafka_workflows.py`:

- `opsyntra_kafka_batch`: health checks → publish full CSV → bounded prediction → summary.
- `opsyntra_kafka_streaming`: health checks → managed consumer and optional sample producer → summary.

Both use the existing ML runtime (install `requirements/kafka.txt` there with
`..\scripts\windows\start-consumer.ps1 -SetupSparkClient`), retries, timeouts, frozen models,
separate groups and run-specific recovery/artifacts. Batch is daily with no
catchup, initially paused; streaming is a manual managed session with heartbeat
logs and graceful stop. Read [bonus operation](../../docs/VERIFICATION.md) for
commands and [successful evidence](../reports/submission_evidence.md) for runs.

`opsyntra_ml_workflow.py` defines the eight-task, manually triggered DAG:

```text
validate_data -> prepare_splits -> tune_and_train -> evaluate
             -> validate_candidate -> save_selected_model -> predict -> save_results
```

## Start and run from backend/

Airflow 3.3.2 runs in the existing Ubuntu WSL distribution with its own Python
3.12 environment. ML task subprocesses use the separate `backend/.venv-spark`
environment, which already contains sklearn and Spark dependencies. The Windows
Python environment continues serving the frontend and recording MLflow runs.

```powershell
# Once on a new machine, after scripts/windows/start-spark.ps1 -Task setup:
..\scripts\windows\start-airflow.ps1 -Task setup

# Start or reuse the local scheduler, DAG processor, API and LocalExecutor.
..\scripts\windows\start-airflow.ps1 -Task start

# Trigger real scheduled tasks and wait for the complete DagRun state.
..\scripts\windows\start-airflow.ps1 -Task run -Engine sklearn
..\scripts\windows\start-airflow.ps1 -Task run -Engine spark

# Optional alternate CSV; path is relative to backend/ and Churn is unnecessary.
..\scripts\windows\start-airflow.ps1 -Task run -Engine sklearn -PredictionInput examples/customers.csv

..\scripts\windows\start-airflow.ps1 -Task status
..\scripts\windows\start-airflow.ps1 -Task imports
..\scripts\windows\start-airflow.ps1 -Task stop
```

From the repository root use `.\scripts\windows\start-airflow.ps1` instead. Open
http://localhost:8080, choose **opsyntra_ml_workflow**, and select a run. Grid,
Graph, Task Instances and individual task logs show the actual task states.
The local development UI binds to loopback and uses Airflow's simple automatic
admin login. It is not a shared production authentication configuration.

Airflow and its SQLite scheduler state live in the Ubuntu account's native
filesystem under `~/.local/share/opsyntra/`; `.airflow-runtime/service.json`
records the exact environment, state directory and process. Project outputs stay
under `backend/artifacts/airflow/`. Avoid installing Airflow into Windows Python
or the ML environment; its official constrained dependencies are separate.

## What each task does

| Task | Work and output |
|---|---|
| validate_data | Verify canonical checksums and schema; freeze raw data, memberships, prediction CSV and configuration in a run snapshot |
| prepare_splits | Normalize deterministically, join the existing customer memberships, write run-local splits; no new random split or fitted preprocessing |
| tune_and_train | Training-only 3-fold parameter search, fitting preprocessing inside each fold; save each complete candidate pipeline |
| evaluate | Reload candidates, choose thresholds using validation F1, rank by validation average precision; evaluate only the winner on held-out test customers |
| validate_candidate | Require finite validation scores, improvement over dummy, configured minimum AP, valid threshold, matching dataset/split/artifact hashes and successful reload |
| save_selected_model | Copy only the approved, unchanged complete pipeline to the run's selected directory |
| predict | Load that saved pipeline in a separate task process and predict the snapshotted raw CSV without Churn |
| save_results | Validate prediction checksum, save final CSV and summary; update the engine's latest successful workflow output |

The dummy participates in sklearn comparisons; Spark runs Logistic Regression,
Random Forest and SparkXGBClassifier. Spark uses a prevalence-score dummy AP for
its approval comparison. Test scores never choose or approve a candidate.

## Operational behavior

`backend/config.yaml` defines retries (2), retry delay (20 seconds), task timeout
(30 minutes), DAG timeout (120 minutes), prediction input and validation policy.
The DAG permits one active run and one active task. Failed upstream tasks block
downstream saving. BashOperator passes engine/input/run ID via environment
variables, and launches a module with a fixed command. Models and datasets are
passed through files, with no large XCom payloads or ML imports during DAG parsing.

Each run has a sanitized, hashed ID and its own directory:

```text
artifacts/airflow/runs/<engine>/<safe-run-id>-<hash>/
  snapshot.json, config.yaml
  source/                  Frozen inputs and authoritative membership map
  data/                    Normalized rows and reused partitions
  training/, training.json Complete fitted candidate pipelines and CV scores
  evaluation/, evaluation.json
  approval.json            Validation and integrity checks
  selected/, selected_model.json
  predictions/             Pending batch output
  results/                 Final predictions.csv and summary.json/.md
  logs/                    Per-task logs and per-attempt status/timing JSON
  mlflow.json              Host tracking IDs after the wrapper records the run
```

Tasks reuse a verified snapshot on retry; retraining/evaluation writes new attempt
subdirectories and publishes its manifest only when complete. Final CSV writes
replace a temporary file, so retrying does not append duplicate customer rows.
`artifacts/airflow/latest_success_<engine>.json` points to the last saved workflow
result. The frontend's existing production model is not automatically promoted;
Airflow saves an approved model for each run.

The PowerShell run command subsequently logs a parent workflow run and nested
candidate runs in the existing **Opsyntra-Airflow** MLflow experiment. This host
step keeps artifact paths accessible from the Windows MLflow UI. Spark candidates
are logged as ZIP archives of their complete native pipeline directories to avoid
Windows deep-path copy limits; custom stage modules and pinned requirements are
in the same candidate run. Extract the ZIP in Linux and load its directory with
`PipelineModel.load` with the Opsyntra code on the Python path. If interrupted
or triggered through the Airflow UI, export the successful run and log it with:

```powershell
..\scripts\windows\start-airflow.ps1 -Task record -Engine sklearn
..\scripts\windows\start-airflow.ps1 -Task track -Engine sklearn
```

Use `-Engine spark` for Spark. Recording verifies Airflow's eight task states and
final DagRun success. Tracking an already-recorded run does not duplicate it.

See [the completed-run report](../reports/airflow_workflow_report.md) and
[shared screenshot evidence](../../docs/README.md). Recreate screenshots with
`node docs/scripts/capture-airflow.mjs` from the repository root after successful
runs. See [Airflow installation](https://airflow.apache.org/docs/apache-airflow/3.3.2/start.html)
and [DAG best practices](https://airflow.apache.org/docs/apache-airflow/3.3.2/best-practices.html).
