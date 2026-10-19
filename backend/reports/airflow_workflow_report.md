# Airflow ML workflow checkpoint

Both engines completed all eight tasks through the actual Airflow 3.3.2 scheduler and LocalExecutor in Ubuntu WSL.
These are scheduled DagRuns, with saved task states, native model artifacts and actual batch predictions.

```text
validate_data -> prepare_splits -> tune_and_train -> evaluate
             -> validate_candidate -> save_selected_model -> predict -> save_results
```

| Engine | DAG state | Successful tasks | Selected model | Validation AP | Threshold | Test F1 | Saved predictions |
|---|---|---:|---|---:|---:|---:|---:|
| sklearn | success | 8/8 | random_forest | 0.662603 | 0.276342 | 0.620833 | 2 |
| spark | success | 8/8 | random_forest | 0.661753 | 0.259173 | 0.623431 | 2 |

Model ranking uses validation average precision; thresholds maximize validation F1. Only the selected model receives held-out test scores. Test scores are not an approval gate.

## Completed runs and saved outputs

### sklearn

Run ID: `manual__opsyntra_sklearn_20261019T111944416960Z`.
Started: 2026-10-19T11:19:46.103521+00:00; finished: 2026-10-19T11:21:09.044407+00:00 (UTC).

- [Final prediction CSV](../artifacts/airflow/runs/sklearn/manual__opsyntra_sklearn_20261019T111944416960Z-4cd927197f52/results/predictions.csv)
- [Result summary](../artifacts/airflow/runs/sklearn/manual__opsyntra_sklearn_20261019T111944416960Z-4cd927197f52/results/summary.md)
- [Candidate approval checks](../artifacts/airflow/runs/sklearn/manual__opsyntra_sklearn_20261019T111944416960Z-4cd927197f52/approval.json)
- [Selected model metadata](../artifacts/airflow/runs/sklearn/manual__opsyntra_sklearn_20261019T111944416960Z-4cd927197f52/selected_model.json)
- [Per-task logs](../artifacts/airflow/runs/sklearn/manual__opsyntra_sklearn_20261019T111944416960Z-4cd927197f52/logs/)
- MLflow experiment: **Opsyntra-Airflow**; workflow run `b4e4f4b0722c4f5b9bdd180e9b05320d`.

![Actual successful sklearn task grid](../../docs/screenshots/airflow-grid-sklearn.png)

![Actual successful sklearn task graph](../../docs/screenshots/airflow-graph-sklearn.png)

[Successful final task log](../../docs/screenshots/airflow-log-sklearn.png)

### spark

Run ID: `manual__opsyntra_spark_20261019T112137235065Z`.
Started: 2026-10-19T11:21:39.958442+00:00; finished: 2026-10-19T11:28:51.777340+00:00 (UTC).

- [Final prediction CSV](../artifacts/airflow/runs/spark/manual__opsyntra_spark_20261019T112137235065Z-33208e8a2aa4/results/predictions.csv)
- [Result summary](../artifacts/airflow/runs/spark/manual__opsyntra_spark_20261019T112137235065Z-33208e8a2aa4/results/summary.md)
- [Candidate approval checks](../artifacts/airflow/runs/spark/manual__opsyntra_spark_20261019T112137235065Z-33208e8a2aa4/approval.json)
- [Selected model metadata](../artifacts/airflow/runs/spark/manual__opsyntra_spark_20261019T112137235065Z-33208e8a2aa4/selected_model.json)
- [Per-task logs](../artifacts/airflow/runs/spark/manual__opsyntra_spark_20261019T112137235065Z-33208e8a2aa4/logs/)
- MLflow experiment: **Opsyntra-Airflow**; workflow run `e86fb2031d99497fb40f58557430705b`.

![Actual successful spark task grid](../../docs/screenshots/airflow-grid-spark.png)

![Actual successful spark task graph](../../docs/screenshots/airflow-graph-spark.png)

[Successful final task log](../../docs/screenshots/airflow-log-spark.png)

## Scheduled task duration

These durations include subprocess startup and orchestration overhead. They are not the engine fit/inference benchmark from Step 4.

| Task | sklearn seconds | Spark seconds |
|---|---:|---:|
| validate_data | 8.946 | 8.380 |
| prepare_splits | 7.843 | 9.439 |
| tune_and_train | 19.473 | 226.116 |
| evaluate | 8.867 | 118.621 |
| validate_candidate | 7.902 | 9.231 |
| save_selected_model | 8.854 | 10.617 |
| predict | 7.898 | 36.174 |
| save_results | 7.539 | 7.912 |

## Reproducibility and operational checks

- Frozen raw data, prediction input, configuration and membership files are checksum-verified by downstream tasks.
- Existing 4,225/1,409/1,409 customer memberships are reused; training folds alone fit imputation, encoding, scaling and classifiers.
- Both approved models passed save/reload probability and label checks. The predict task loaded the approved artifact in a separate process and accepted CSV rows without Churn.
- Candidate gates check validation metrics, dummy improvement, threshold range, data identity and artifact integrity. Approved artifacts are copied into the run's selected directory.
- Retries: 2, with a 20-second delay. Per-task timeout: 30 minutes; DAG timeout: 120 minutes. One active run and one active task avoid overlapping local training.
- Every task writes a log and per-attempt status/timing JSON. Failed upstream tasks block downstream outputs; retries replace final CSVs rather than appending duplicates.
- Run artifacts are isolated by engine and sanitized, hashed run ID. The application frontend's model is not automatically replaced.
- The workflow and all seven candidate runs are recorded in the existing Windows MLflow store, keeping artifact paths accessible from its UI.

Dataset SHA256: `88be4b93fbe0cc83421af1c503794c97c342eca914c1576db7c276e61d61358a`.
Membership SHA256: `b51f7b090312e8c727b11753dd740acdadad3e47d01c2261fe76fb5421b4e82c`.

The JSON report retains scheduler task states, attempt events, approvals, model/data identifiers, predictions and MLflow IDs. Screenshots show the real local UI and are linked to scheduler run IDs in `docs/screenshots/airflow-evidence.json`.

See [DAG setup and commands](../dags/README.md). Airflow's local automatic admin login is bound to loopback for development; shared deployment requires a separate deployment/authentication configuration.
