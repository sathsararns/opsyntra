# Grouped application package verification

Verified on 26 October 2026 (Asia/Colombo). Result: **PASS**.

Moved 27 flat application modules into `data/`, `ml/`, `spark/`, `kafka/`,
`orchestration/` and `api/`. Existing `analytics/` remains alongside them.
The frontend, workflow command names and data/artifact locations are retained.
The [source guide](../src/opsyntra/README.md) describes the reading order.

| Check | Result |
|---|---|
| Backend core regression | 98/98 passed; zero skipped |
| Real WSL Spark regression | 4/4 passed; LR, RF and XGBoost persistence/inference |
| Historical Joblib class path | Fresh-process reload produces identical predictions |
| Historical Spark stage paths | Saved legacy stage names reload in fresh single/batch CLI processes |
| MLflow export | A fresh process loads one fixture candidate using its bundled package source |
| Frontend | Production build and TypeScript check passed |
| Browser regression | 9/9 passed; synthetic model/API fixture and controlled analytics responses |
| Existing local Docker stack | Six healthy services; initialization jobs completed |
| Real Kafka-to-PostgreSQL flow | Three acknowledged demo events produce three raw rows and three logical predictions |
| Existing approved models | Native version `d9a13b4cf6839ed4`; Docker version `34e84e9fa5d1de06` |
| Preserved artifacts | 378 data/model/recovery files have unchanged SHA256 values |
| Static validation | 121 Python files parse; internal application import paths resolve; eight PowerShell launchers parse |

`_compat.py` provides lazy aliases for historical module names without adding
duplicate business-logic files. New imports use the grouped packages. MLflow
exports include the complete package, and Spark source logging preserves that
hierarchy. The Docker selected model checksum stayed
`cb4140879f32e3a606f611b56be5d8c5f613c44dcdea483a3a62023ac3a699c2`.

The verification stack was stopped with `docker compose down`; named volumes,
models, analytics rows and recovery state were retained. The three bounded demo
events remain in its Kafka/PostgreSQL history. Restart through the normal
`scripts/windows/start-containers.ps1 -Task start` command. Native Python
services should also be restarted after updating their source checkout.

Full Airflow scheduler runs, full production retraining and clean-clone setup
were not repeated. Core Airflow task contracts passed. Notebook imports were
updated while all historical notebook outputs and existing reports were kept.
The browser fixture suite does not establish real-stream refresh latency; the
separate Docker probe verifies actual event ingestion and prediction rows.

Initial sandbox attempts encountered temporary-directory and worker-process
permission errors. Successful regression/build runs used normal local execution.
Spark's negative-input test intentionally logs a rejected numeric-value error;
the test suite finished successfully. Resource warnings were retained in its
completion log.

See the [machine-readable report](package_refactor_verification.json) and
[retained evidence](../../docs/evidence/package-refactor/20261026/).
