# Developer tools

Run these modules from `backend/`. Reusable application logic remains under
`src/opsyntra/`; the main ML commands remain under `pipelines/`.

| Folder | Responsibility | Example |
|---|---|---|
| `ci/` | Synthetic fixtures, CI test profiles and ephemeral service checks | `python -m tools.ci.ci_checks --profile core` |
| `verification/` | Backend, database, Kafka, browser, notebook and clean-clone checks | `python -m tools.verification.runner` |
| `evidence/` | Capture/tracking, submission reports and model-bundle handoff | `python -m tools.evidence.model_bundle --help` |
| `services/` | Native service operations, topic/window inspection, setup and UI snapshot tools | `python -m tools.services.kafka_messages --help` |

The eight workflow entrypoints in `pipelines/` are `data_pipeline`,
`training_pipeline`, `inference_pipeline`, `engine_comparison`,
`spark_training_pipeline`, `spark_inference_pipeline`, `airflow_task` and
`kafka_airflow_task`.

## Command migration

The following commands previously lived under `pipelines`. Their module paths
now use the group shown below. There are no forwarding copies in `pipelines/`.

| New prefix | Module names |
|---|---|
| `tools.ci.` | `ci_api`, `ci_checks`, `ci_database`, `ci_kafka`, `ci_prepare` |
| `tools.verification.` | `analytics_check`, `analytics_consumer_check`, `analytics_dashboard_check`, `clean_clone_check`, `dashboard_event`, `kafka_consumption_check`, `kafka_publishing_check`, `notebook_check` |
| `tools.verification.runner` | The previous `pipelines.verification` test runner |
| `tools.evidence.` | `airflow_evidence`, `airflow_tracking`, `capture_kafka_evidence`, `model_bundle`, `phase1_evidence`, `spark_tracking`, `submission_evidence` |
| `tools.services.` | `airflow_service`, `analytics_db`, `analytics_local`, `frontend_snapshot`, `kafka_messages`, `kafka_service`, `kafka_topics`, `kafka_window`, `mlflow_server`, `spark_environment` |

Windows launcher locations and their arguments are unchanged. They invoke the
new module paths internally. Docker images copy this package alongside
`pipelines/`, so runtime topic initialization and bonus Kafka DAGs can import
their service helpers.

Root path resolution in relocated modules still points at the same `backend/`
or repository directory. Dataset, model, report and recovery-state locations
are unchanged. The serialized model package is still named `opsyntra`.

Captured logs, run manifests and source hashes in historical evidence retain
the paths used at the time. Current commands, imports, tests and setup guides
use the new locations. A fresh clean-clone verification requires a commit
containing this refactor; an older checkout will not contain `tools/`.
