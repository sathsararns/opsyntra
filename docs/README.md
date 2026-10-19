# Shared project documentation

- [Project roadmap](../OPSYNTRA_ROADMAP.md)
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

To refresh evidence after training, start `.\start-mlflow.ps1` from the repository
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
