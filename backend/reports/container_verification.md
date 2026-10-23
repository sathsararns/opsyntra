# Phase 7 container verification

**PASS — 2026-10-21.** The core application was built and run in a new isolated
Docker Compose project using the original 7,043-customer CSV. No existing WSL
application services, host models or analytics databases were replaced.

| Check | Observed result |
| --- | --- |
| `docker compose up --build` | API, frontend, Kafka, PostgreSQL and both consumers became healthy; three initialization jobs exited 0 |
| Real producer → prediction → analytics → frontend | 3 raw events, 3 logical predictions; SQL and Next.js live screen agreed |
| Traceability | Browser displayed actual event ID, prediction ID and model version; screenshot saved |
| Production overview and direct inference | 7,043 customers; selected Random Forest; single/CSV probabilities and labels identical without `Churn`; blank `TotalCharges` accepted |
| Restart | Logical rows, selected-model SHA-256, input offsets and SQLite outbox preserved |
| Container removal/recreation | `down` followed by `up` preserved the same results and recovery state through named volumes |
| Duplicate replay | 3 original raw messages republished: raw count stayed 3, prediction count stayed 3, outbox stayed 3; physical delivery records grew from 6 to 9 |
| Least privilege | API reader could SELECT and could not INSERT predictions |
| Linux backend image regression | **95/95**, zero skipped, Python 3.12.15 |
| Windows regression | **95/95**, zero skipped, Python 3.14.6 |
| Spark image integration | **4/4**, zero skipped, including Spark XGBoost, preprocessing, persistence and fresh-process inference |
| MLflow profile | Healthy HTTP endpoint and persisted `Opsyntra-Telco-sklearn` experiment visible through search API |
| Airflow profile | Image built; metadata, scheduler, triggerer and DAG processor healthy; 3 DAGs/16 tasks imported; correct ML interpreter on all tasks |
| Airflow shared-path probes | `validate_data` and `prepare_splits` executed successfully in the ML environment against the shared volumes |
| Final cleanup | Isolated containers stopped/removed; named volumes retained; normal application project not started |

Selected container model version: `34e84e9fa5d1de06`. The saved pipeline was
trained inside the Linux image, rather than importing a Windows pickle. Its
SHA-256 remained `cb4140879f32e3a606f611b56be5d8c5f613c44dcdea483a3a62023ac3a699c2`.

The measured interval from starting the bounded producer command until the
Next.js proxy reported all three predictions was **4.54 seconds**. This includes
the short producer-container startup. The separate browser assertion verified
rendered values and IDs; this measurement is not a general browser-latency SLA.
Synthetic replay remains a simulation, not observed company churn.

## Evidence

- [Final core acceptance manifest](../../docs/evidence/containers/opsyntra-verify-20261021064526/verification.json)
- [Actual live screen](../../docs/evidence/containers/opsyntra-verify-20261021064526/live-monitoring.png)
- [SQL before restart](../../docs/evidence/containers/opsyntra-verify-20261021064526/before.txt), [after recreation](../../docs/evidence/containers/opsyntra-verify-20261021064526/after-recreate.txt), [after replay](../../docs/evidence/containers/opsyntra-verify-20261021064526/after-replay.txt)
- [Linux regression JSON](../../docs/evidence/containers/runtime/linux-core.json), [Windows regression JSON](../../docs/evidence/containers/runtime/windows-core.json), [Spark regression JSON](../../docs/evidence/containers/runtime/spark.json)
- [Direct prediction/API results](../../docs/evidence/containers/runtime/prediction-api.json), [MLflow HTTP results](../../docs/evidence/containers/runtime/mlflow.json), [Airflow health and task probes](../../docs/evidence/containers/runtime/airflow.json)
- [Image IDs](../../docs/evidence/containers/runtime/images.txt) and [aggregate manifest with hashes](container_verification.json)

Earlier timestamped directories retain failed development checks. The final
acceptance project above is the authoritative successful core run. Optional
Airflow startup/health configuration was finalized after that core run and was
then tested separately; the aggregate manifest retains both source hashes.
Spark logs include an intentional invalid-numeric-input exception exercised by
a passing rejection test.

## Scope and reproduction

See [Docker startup, stop/reset, profiles and recovery guide](../../docs/CONTAINERS.md).
Run `python containers/verify.py` from root for a new isolated core acceptance
project. Raw data and model artifacts remain excluded from Git/image contexts;
credentials are generated into ignored local environment files.

Docker Desktop 4.91.0 / Engine 29.8.0 / Compose 5.5.1, Linux amd64, approximately
3.7 GiB Docker memory were used. Heavy image extraction/training was slow on
this allocation. The Airflow profile checks cover a healthy standalone service,
DAG import, interpreter configuration and two task entry points; a complete
eight-task scheduler run was **not repeated inside Docker**. Existing WSL DAG
run evidence remains separate. Cloud deployment and hosted GitHub Actions
verification are outside this phase.
