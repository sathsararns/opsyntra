# Shared project documentation

- [Complete project roadmap: ML, Kafka, analytics, CI/Docker and optional cloud](../OPSYNTRA_ROADMAP.md)
- [Phase 1 clean-clone verification](../backend/reports/clean_clone_verification.md)
- [Frontend typecheck/build/browser evidence](../backend/reports/frontend_verification.json)
- [Submission demo guide](DEMO_GUIDE.md)
- [Final local Docker demo checks and fresh screenshots](../backend/reports/local_demo_verification.md)
- [Phase 2 analytics storage, setup and metric definitions](ANALYTICS_STORAGE.md)
- [Real PostgreSQL verification](../backend/reports/analytics_verification.md)
- [Phase 3 Kafka analytics consumer and recovery](ANALYTICS_CONSUMER.md)
- [Real Kafka/PostgreSQL crash and replay evidence](../backend/reports/analytics_consumer_verification.md)
- [Actual application backlog and completed restart](../backend/reports/analytics_application_verification.md)
- [Phase 4 live monitoring, APIs and metric definitions](LIVE_MONITORING.md)
- [Live dashboard browser/SQL checks and measured latency](../backend/reports/live_dashboard_verification.md)
- [Live monitoring desktop](screenshots/phase4/live-monitoring.png)
- [Live monitoring mobile](screenshots/phase4/live-monitoring-mobile.png)
- [Phase 5 notebook setup, inputs and snapshot interpretation](NOTEBOOKS.md)
- [Executed EDA/model/analytics notebooks](../backend/notebooks/README.md)
- [Notebook execution and SQL agreement evidence](../backend/reports/notebook_verification.md)
- [Phase 6 CI workflows, local commands and hosted completion](CI.md)
- [Local CI checks and deliberate-failure proof](../backend/reports/ci_verification.md)
- [Phase 7 Docker/Compose setup, profiles and persistent recovery](CONTAINERS.md)
- [Container acceptance evidence](../backend/reports/container_verification.md)
- [Separate saved-model setup evidence](../backend/reports/model_bundle_verification.json)
- [Frontend setup](../frontend/README.md)
- [Backend and ML workflows](../backend/README.md)
- [Desktop screenshot](screenshots/frontend-desktop.png)
- [Mobile screenshot](screenshots/frontend-mobile.png)
- [Customer prediction screenshot](screenshots/frontend-prediction.png)
- [MLflow completed runs](screenshots/mlflow-runs.png)
- [MLflow model comparison](screenshots/mlflow-comparison.png)
- [Selected model, threshold and test metrics](screenshots/mlflow-selected-model.png)
- [Model selection explanation](../backend/reports/model_selection.md)
- [Spark and sklearn comparison](../backend/reports/engine_comparison.md)

`screenshots/` contains shared interface evidence. Machine-readable ML validation
and evaluation reports belong in `backend/reports/`.

The MLflow comparison shows four runs from the same training batch and saved
partitions. Random Forest has the highest validation average precision (0.6626).
Its validation-F1 threshold is 0.276342. The selected-run screenshot explains this
choice and shows the final test metrics. It catches more churners at the cost of
more false positives; the threshold is a starting F1 policy, not a cost model.
Missing test metrics for other candidates are intentional: test data did not
select the model or threshold.

`screenshots/mlflow-evidence.json` links the images to the exact batch, model
version, dataset/split hashes, run IDs, and test metrics. Images show the actual
local MLflow UI. Its run Duration covers artifact logging; `training_seconds`
records model training and CV time.

To refresh evidence after training, start `.\scripts\windows\start-mlflow.ps1` from the repository
root, then run in another terminal:

```powershell
cd frontend
npm run evidence:mlflow
```

The capture script uses installed Microsoft Edge on Windows and the current
`backend/reports/model_training_report.json`. MLflow models, Joblib pipelines,
confusion matrices, ROC/PR curves, threshold searches and configuration are
available under each run's Models/Artifacts views. See the current selection
report for the batch-specific plot directory.

## Airflow execution evidence

- [Completed workflow report](../backend/reports/airflow_workflow_report.md)
- [Scheduler task states, approvals and outputs](../backend/reports/airflow_workflow_report.json)
- [Successful sklearn Grid](screenshots/airflow-grid-sklearn.png)
- [Successful sklearn Graph](screenshots/airflow-graph-sklearn.png)
- [Successful sklearn result task log](screenshots/airflow-log-sklearn.png)
- [Successful Spark Grid](screenshots/airflow-grid-spark.png)
- [Successful Spark Graph](screenshots/airflow-graph-spark.png)
- [Successful Spark result task log](screenshots/airflow-log-spark.png)
- [Screenshot provenance with both DagRun IDs](screenshots/airflow-evidence.json)

These images show actual Airflow 3.3.2 scheduler runs in Ubuntu WSL. Each run
completed all eight tasks and saved two example-customer predictions using its
approved persisted model. The result task log records the actual output folder.
The Grid shows task states and timing; Graph shows the required dependency chain.

To refresh evidence, complete both engine runs, keep Airflow running on port 8080,
then run from the repository root:

```powershell
node docs/scripts/capture-airflow.mjs
```

From `backend/`, rebuild the hash-verified report with
`python -m pipelines.airflow_evidence`. The PowerShell launcher records successful
workflow/candidate runs in the **Opsyntra-Airflow** MLflow experiment.

Verification performed for this checkpoint: the complete host suite ran 41 tests
(37 passed, four Spark-runtime checks skipped on Windows). After the final
Windows path fix, all five Airflow workflow/integrity checks passed, including a
regression for native model files beyond the normal Windows path limit. Both
engines also completed real eight-task scheduler runs. Downloaded MLflow Spark
archives were checked against the complete original native model checksums.

## Kafka publishing evidence

- [Publishing setup and delivery policy](../backend/KAFKA.md)
- [Real broker batch/streaming checkpoint report](../backend/reports/kafka_publishing_report.md)
- [Wire samples, producer run IDs and partition offset evidence](../backend/reports/kafka_publishing_report.json)

The broker check published the complete source CSV through a pause/resume
sequence and 30 sampled streaming events. A diagnostic reader validated the
actual raw-topic messages, keys, numeric/null types, event IDs and source values.
Command logs and the acknowledged batch checkpoint are linked from the report.
Prediction-topic production and dead-letter routing are implemented in Step 7.

## Kafka prediction and recovery evidence

- [Consumer commands, output contract and recovery limits](../backend/KAFKA.md#prediction-consumer-step-7)
- [Real-broker prediction and crash/restart report](../backend/reports/kafka_consumption_report.md)
- [Group/run IDs, model hashes and actual prediction/dead-letter samples](../backend/reports/kafka_consumption_report.json)

The check exercised malformed records, count/window bounds, resumed groups, event
ID conflicts, duplicate suppression and hard process exits at both delivery/ledger
boundaries. Sklearn output matched direct saved-model inference. Native Spark
batch and bounded streaming also published real predictions. The message viewer
lets you inspect output JSON for terminal screenshots without changing application
offsets: `python backend/pipelines/kafka_messages.py --topic predictions --max-messages 5`
from the repository root.

## Final tests and bonus Kafka DAG evidence

- [Reproduction, tests, capture commands and limits](VERIFICATION.md)
- [Complete rubric/evidence matrix](../backend/reports/submission_evidence.md)
- [Hash-indexed evidence and bonus task states](../backend/reports/submission_evidence.json)
- [Host test results](../backend/reports/test_results.json)
- [Native Spark test results](../backend/reports/test_results_spark.json)
- [Kafka input/output CLI transcripts](screenshots/kafka-input-output.png)
- [Malformed messages](screenshots/kafka-deadletter.png)
- [Durable-ledger recovery](screenshots/kafka-recovery.png)
- [Ack-before-ledger replay window](screenshots/kafka-replay-window.png)
- [Bonus batch Grid](screenshots/kafka-airflow-batch-grid.png)
- [Bonus streaming Grid](screenshots/kafka-airflow-streaming-grid.png)
- [Consumer heartbeat and shutdown log](screenshots/kafka-airflow-streaming-log.png)
- [Screenshot provenance](screenshots/kafka-evidence.json)

Kafka message/recovery screenshots are labelled views of actual recorded CLI
output, retained in `docs/evidence/`. Bonus DAG screenshots show the actual
Airflow UI. Current test counts are in the evidence matrix; earlier checkpoint
counts above describe those earlier checkpoints only.
