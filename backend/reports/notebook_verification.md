# Phase 5 notebook verification

Status: **PASS**.

Command from backend: `python -m pipelines.notebook_check`. Kernels use the invoking Python environment.

| Notebook | Executed code cells | Exports | Status |
|---|---:|---:|---|
| 01_telco_eda.ipynb | 6 | 12 | PASS |
| 02_model_comparison.ipynb | 6 | 15 | PASS |
| 03_streaming_analytics.ipynb | 5 | 17 | PASS |

Input/SQL/API checks: 5/5 passed; 0 skipped.

No training or tuning calls; no held-out test feature CSV is read. Existing final test metrics are displayed as recorded assessments. Validation curves are checked against saved AP/ROC-AUC. Figure/table export hashes and credential scans passed only when status is PASS.

Streaming analysis reads the application database with the reader role in one repeatable-read transaction; real integration fixtures use the separate test database. Its scope is engine/model-specific and excludes the current partial UTC hour. Snapshot CSVs and metadata preserve the inputs; rerunning against a live database can change results.

Synthetic streaming replay is a simulation, not evidence of real company churn trends or observed model accuracy.

See [setup and interpretation](../../docs/NOTEBOOKS.md), [JSON results and SHA-256 hashes](notebook_verification.json), and [exported figures/tables](notebooks/).
