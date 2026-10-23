# Reproduce Opsyntra and its submission evidence

Run the PowerShell commands below from **`Opsyntra/`**, the repository root.
Inside `backend/`, use `..\start-*.ps1` instead of `.\start-*.ps1`.
The local setup uses Windows Python for the API/sklearn/MLflow and Ubuntu WSL
Python 3.12 + Java 21 for Spark, Kafka and Airflow. Airflow has its own isolated
environment. See [backend setup](../backend/README.md) for prerequisites.

## Clean checkout

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r backend/requirements/web.txt
python -m pip install -r backend/requirements/kafka.txt
npm --prefix frontend ci
```

Provide the assignment CSV at
`backend/data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv`. Alternatively run
`python -m pipelines.data_pipeline --archive "C:\path\archive.zip"` from
`backend/`. Raw data, service state, saved models and ledgers are ignored by Git;
reports and screenshots are evidence, not substitutes for those artifacts.

The dataset source referenced by the assignment is
[Kaggle Telco Customer Churn](https://www.kaggle.com/datasets/blastchar/telco-customer-churn).
For checksum-exact reproduction, use the supplied original archive. The expected
CSV SHA-256 is `88be4b93fbe0cc83421af1c503794c97c342eca914c1576db7c276e61d61358a`.
Compare the CSV bytes after extraction; a newly downloaded archive can have a
different ZIP hash even when its CSV is identical. The repository deliberately
does not bundle raw data, model binaries or a complete MLflow tracking database.

```powershell
Push-Location backend
python -m pipelines.data_pipeline
python -m pipelines.training_pipeline
Pop-Location
.\scripts\windows\start-spark.ps1 -Task setup
.\scripts\windows\start-spark.ps1 -Task train
.\scripts\windows\start-airflow.ps1 -Task setup
.\scripts\windows\start-kafka.ps1 -Task setup
.\scripts\windows\start-kafka.ps1 -Task start
.\scripts\windows\start-consumer.ps1 -SetupSparkClient
.\scripts\windows\start-airflow.ps1 -Task start
```

The setup launchers require an installed Ubuntu WSL distribution and access to
official package sources. Saved stratified memberships are shared by both
engines. Training-only CV fits preprocessing; validation AP selects the model,
validation F1 selects its threshold, and test data selects neither.

For new installations, Spark setup creates its Python environment in native
Linux storage under `~/.local/share/opsyntra/` and links the familiar
`backend/.venv-spark` path to it. This avoids slow package installation/imports
through the Windows filesystem. Existing `.venv-spark` environments are preserved.

Airflow setup installs `requirements/worker.txt` into the separate
`backend/.venv-spark` runtime as well as installing constrained Airflow into its
own environment. The worker needs MLflow/matplotlib for ML task artifacts and
the Kafka client for bonus tasks; the scheduler does not execute ML code in its
own constrained environment. On an existing setup, install the worker file
directly with that Linux environment's Python if those packages are missing.

## Phase 1 clean-clone verification

```powershell
Push-Location backend
python -m pipelines.clean_clone_check --archive "C:\Users\SATHSARA\Downloads\archive.zip" --include-airflow
python -m pipelines.phase1_evidence
Pop-Location
```

Change the archive path on another computer. The verifier creates an independent
Git clone under ignored `.verification/`, new Windows and Linux ML environments,
and fresh `node_modules`. It imports the supplied archive, retrains both engines,
executes single/CSV inference, backend/Spark/browser tests, and Kafka recovery
checks. With `--include-airflow`, it temporarily stops the original Airflow
service, executes both eight-task ML DAGs with fresh clone-specific metadata,
then restores the original service. Allow the full command to finish.

The clone starts at Git **HEAD**, not the uncommitted working tree. Commit the
intended application fixes before starting a new verification. The Phase 1
report identifies its baseline commit and an exact, retained Spark logging
portability patch applied after the first attempt exposed a long-path failure.
It does not silently treat pending source changes as committed code.

The existing WSL/Java/Node/Edge installation, package caches, Kafka broker and
Airflow tool installation are explicitly recorded as reused prerequisites. This
is a clean application clone, not a new operating system. Main models and
tracking storage remain separate from the clone. If a stage fails, the report
retains its log; use `--resume <verification-directory>` with the same archive
and source commit after correcting the cause. Do not present a RUNNING/FAIL
report as successful evidence.

The retained [clean-clone report](../backend/reports/clean_clone_verification.md),
its JSON companion, [frontend report](../backend/reports/frontend_verification.json)
and [demo sequence](DEMO_GUIDE.md) are the Phase 1 handoff documents.

## Optional saved-model setup

This path is labelled separately from the clean rebuild. Export a bundle only
after training both engines, choosing a new output name:

```powershell
Push-Location backend
python -m pipelines.model_bundle --export artifacts/releases/opsyntra-models.zip
python -m pipelines.model_bundle --verify artifacts/releases/opsyntra-models.zip
Pop-Location
```

The ZIP contains the selected sklearn pipeline, complete native Spark pipeline,
selection metadata and model/comparison reports. It includes a member manifest
and an adjacent `.zip.sha256` file. It does not contain the CSV, credentials,
Python packages, consumer checkpoints or the MLflow tracking database. The
verifier rejects checksum mismatches, unexpected paths and duplicate entries.

To use a supplied bundle, install compatible dependencies in a separate checkout
and run the data pipeline on the original archive first. Confirm that its raw
CSV and saved membership hashes match `bundle_manifest.json`. Verify the ZIP
checksum against the supplied handoff hash, then use the guarded restore command
from the checkout that contains the bundle tool:

```powershell
Push-Location backend
python -m pipelines.model_bundle --restore artifacts/releases/opsyntra-models.zip --backend "C:\path\separate-checkout\backend"
Pop-Location
```

Restore checks the actual destination dataset/membership hashes, refuses
conflicting existing model artifacts, and supports long Windows native Spark
filenames. Ordinary ZIP extraction can fail at the Windows path-length limit.
Then run the normal single/CSV inference commands in the destination checkout.
Use bundles from a trusted source, as Joblib contains serialized Python objects.

The saved-model path speeds up inference setup but does not recreate MLflow
experiments or demonstrate retraining. Run training normally when demonstrating
model selection, experiment artifacts and the tracking UI.

## Tests and real recovery checks

```powershell
Push-Location backend
python -m pipelines.verification
Pop-Location
wsl -d Ubuntu --cd /mnt/c/Users/SATHSARA/Desktop/Opsyntra/backend --exec .venv-spark/bin/python -m pipelines.verification --spark-only
```

Change the WSL path to your checkout on another computer. Results/logs are
`backend/reports/test_results.json`, `test_results.log`, `test_results_spark.json`
and `test_results_spark.log`. Spark tests intentionally skipped on Windows must
pass in the actual Spark runtime. Coverage includes missing/corrupt fields,
unseen categories, leakage prevention, reload, single/CSV/Kafka probability
agreement, both modes, checkpoint resume, partial delivery, manual commits,
invalid messages, event-ID conflicts and crash recovery.

The model encoder supports unknown categories. The Kafka wire schema deliberately
rejects categories outside its documented vocabulary; these are separate policies.

```powershell
.\scripts\windows\start-kafka.ps1 -Task verify
Push-Location backend
python -m pipelines.kafka_consumption_check --include-spark
Pop-Location
```

These live checks publish their own events, including deliberately malformed
records, and use separate groups/windows. Ordinary consumers can also see and
deadletter those records. Exit **86** in `crash_*.log` is an intentional hard exit;
the completed recovery check must return PASS. Reuse the same consumer group and
state directory on restart. Delivery is **at-least-once**, including the tested
output-ack/before-ledger duplicate window. Downstream sinks should upsert by
`prediction_id`; deduplication is not global across independent groups.

## Required and bonus Airflow workflows

```powershell
.\scripts\windows\start-airflow.ps1 -Task run -Engine sklearn
.\scripts\windows\start-airflow.ps1 -Task run -Engine spark
.\scripts\windows\start-airflow.ps1 -Task run-kafka -KafkaKind batch -Engine sklearn
.\scripts\windows\start-airflow.ps1 -Task run-kafka -KafkaKind streaming -Engine sklearn -DurationSeconds 30 -DemoEvents 20 -EventsPerSec 5
```

The required ML DAG trains and logs to MLflow. Bonus DAGs reuse frozen copies of
the selected model and do not retrain it. They also accept `-Engine spark`;
recorded bonus runs use sklearn, while actual Spark inference/persistence tests
and Kafka inference are verified separately.

```text
opsyntra_kafka_batch:
health_checks -> publish_batch -> predict_batch -> summarize

opsyntra_kafka_streaming:
health_checks -> start_and_monitor_consumer --+
             -> publish_samples ------------+-> summarize
```

Batch has `@daily`, `catchup=False`, fixed start date 2026-10-19 UTC, and starts
paused. Unpause it in Airflow to enable daily processing. The command temporarily
unpauses it and restores its previous pause state. If a daily interval is due,
the scheduler may additionally queue the latest scheduled interval. Every run
has separate artifacts and a checkpoint.

Streaming owns its consumer inside the task process, which stays RUNNING until
the session duration (5–3,600 seconds after model load) or an early stop request.
It records heartbeat logs and `consumer_heartbeat.json` every five seconds.
`max_active_runs=1` prevents overlapping runs; the persistent group lock protects
recovery state. Optional `demo_events` uses the real bounded streaming producer;
the default is zero so live traffic can come from a separate producer.

For an early stop, create an empty `stop.requested` file in the active folder
`backend/artifacts/kafka/airflow/streaming/<run-key>/` shown in the health task.
The consumer finishes output acknowledgements, ledger writes and commits before
returning. SIGTERM/SIGINT also request graceful shutdown. No detached consumer
daemon survives the task. For an unbounded service use standalone
`consumer.py --mode streaming`.

Tasks have two retries, task/DAG timeouts and attempt logs. Retries reuse frozen
data/config/model, checkpoints, group and ledger. Batch fixes its end offsets
after publishing. Its summary verifies one confirmed logical result per source
event and atomically replaces the CSV. Other traffic can fall within the window;
the summary filters this batch's event IDs. A new streaming run starts at the
high watermarks captured by health checks; retries preserve committed offsets.

Outputs under `backend/artifacts/kafka/airflow/<kind>/<run-key>/` include
`manifest.json`, batch `producer_checkpoint.json`/`window.json`, attempt logs,
`recovery/`, `predictions.csv` and `summary.json`. `latest_success_<kind>.json`
records completed outputs. Launcher evidence also verifies all four scheduled
task states. The local development Airflow UI is http://localhost:8080.

## Capture and audit evidence

Run `.\scripts\windows\start-mlflow.ps1` in a separate terminal and keep Airflow/Kafka running.

```powershell
node frontend/scripts/capture-mlflow.mjs
node docs/scripts/capture-airflow.mjs
Push-Location backend
python -m pipelines.capture_kafka_evidence
Pop-Location
node docs/scripts/capture-kafka.mjs
Push-Location backend
python -m pipelines.submission_evidence
Pop-Location
```

Capture uses installed Microsoft Edge through Playwright. MLflow/Airflow images
show the actual UIs. Kafka screenshots are **labelled recorded CLI transcripts**
with source text/logs in `docs/evidence/`, not a Kafka administration UI. Topic
viewers show historical samples without changing application offsets. Provenance
JSON links screenshots to runs/models/commands.

The [evidence matrix](../backend/reports/submission_evidence.md) maps requirements
to code, tests, reports and screenshots. Its JSON companion records sizes,
SHA-256, scheduler states and output CSV hashes. Generation fails if required
tests, native Spark coverage, matching MLflow provenance, bonus successes,
outputs or screenshots are missing.

This is a local single-broker/shared-filesystem LocalExecutor setup. It does not
demonstrate multi-host fault tolerance or a distributed Spark speedup. The
[engine comparison](../backend/reports/engine_comparison.md) records actual local
timings and Spark startup costs.

References: [Airflow tasks/timeouts](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html),
[task best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html),
[Kafka delivery semantics](https://kafka.apache.org/design/).
