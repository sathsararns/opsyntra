# Opsyntra verification and evidence

Generated: 2026-10-20T08:32:42.690095+00:00

Host: 74 passed, 4 Spark checks skipped. Native WSL Spark: 4 passed, no skips. All 78 unique tests passed across the two runtimes.

Model: `d9a13b4cf6839ed4`. Dataset: `88be4b93fbe0cc83421af1c503794c97c342eca914c1576db7c276e61d61358a`. Splits: `b51f7b090312e8c727b11753dd740acdadad3e47d01c2261fe76fb5421b4e82c`.

| Requirement | Evidence / implementation | Screenshot / check |
|---|---|---|
| Missing values, unseen categories, leakage and reload | [test_results.json](../../backend/reports/test_results.json) | [test_model_pipeline.py](../../backend/tests/unit/test_model_pipeline.py) |
| Single, CSV, batch Kafka and streaming Kafka probabilities match | [test_results.json](../../backend/reports/test_results.json) | [test_kafka_consumer.py](../../backend/tests/integration/test_kafka_consumer.py) |
| Native Spark models, unseen categories, persistence and fresh-process inference | [test_results_spark.json](../../backend/reports/test_results_spark.json) | [test_spark_workflow.py](../../backend/tests/integration/test_spark_workflow.py) |
| Comparable MLflow runs and validation-based selection | [model_selection.md](../../backend/reports/model_selection.md) | [mlflow-comparison.png](../../docs/screenshots/mlflow-comparison.png) |
| Same held-out customers and honest engine timings | [engine_comparison.md](../../backend/reports/engine_comparison.md) | [engine_comparison.json](../../backend/reports/engine_comparison.json) |
| Required eight-task ML DAG, both engines | [airflow_workflow_report.md](../../backend/reports/airflow_workflow_report.md) | [airflow-evidence.json](../../docs/screenshots/airflow-evidence.json) |
| Batch/streaming producer and checkpoint resume | [kafka_publishing_report.md](../../backend/reports/kafka_publishing_report.md) | [kafka-input-output.png](../../docs/screenshots/kafka-input-output.png) |
| Invalid records, deduplication and real crash recovery | [kafka_consumption_report.md](../../backend/reports/kafka_consumption_report.md) | [kafka-recovery.png](../../docs/screenshots/kafka-recovery.png) |
| Remaining output-ack/ledger replay window | [restart_after_output_ack.log](../../docs/evidence/restart_after_output_ack.log) | [kafka-replay-window.png](../../docs/screenshots/kafka-replay-window.png) |
| Bonus scheduled batch Kafka DAG | [opsyntra_kafka_workflows.py](../../backend/dags/opsyntra_kafka_workflows.py) | [kafka-airflow-batch-grid.png](../../docs/screenshots/kafka-airflow-batch-grid.png) |
| Bonus managed streaming DAG, heartbeats and graceful stop | [opsyntra_kafka_workflows.py](../../backend/dags/opsyntra_kafka_workflows.py) | [kafka-airflow-streaming-log.png](../../docs/screenshots/kafka-airflow-streaming-log.png) |
| Reproducible setup and operating limits | [VERIFICATION.md](../../docs/VERIFICATION.md) | [KAFKA.md](../../backend/KAFKA.md) |
| Independent clean clone, fresh model training and scheduler runs | [clean_clone_verification.md](../../backend/reports/clean_clone_verification.md) | [clean_clone_verification.json](../../backend/reports/clean_clone_verification.json) |
| Frontend typecheck/build and six real browser flows | [frontend_verification.json](../../backend/reports/frontend_verification.json) | [frontend-prediction.png](../../docs/screenshots/frontend-prediction.png) |
| Submission demonstration and optional saved-model setup | [DEMO_GUIDE.md](../../docs/DEMO_GUIDE.md) | [VERIFICATION.md](../../docs/VERIFICATION.md) |
| Checksum-verified saved-model restore and both engines' inference | [model_bundle_verification.json](../../backend/reports/model_bundle_verification.json) | [model_bundle.py](../../backend/pipelines/model_bundle.py) |
| Isolated native-Linux environment setup and repeat setup | [spark_environment_verification.json](../../backend/reports/spark_environment_verification.json) | [spark_environment.py](../../backend/pipelines/spark_environment.py) |

## Real bonus scheduler runs

- **batch**: `manual__opsyntra_kafka_batch_20261019T154505200671Z`; all four tasks successful; 7043 confirmed prediction rows; model `d9a13b4cf6839ed4`.
- **streaming**: `manual__opsyntra_kafka_streaming_20261019T154755633634Z`; all four tasks successful; 20 confirmed prediction rows; model `d9a13b4cf6839ed4`.

## Phase 1 clean-clone handoff

A separate local clone of `8a027da66ba0772eec2a397bb75ba1a541595ed4` rebuilt both engines in fresh ML environments, passed six real browser flows, and completed both required eight-task DAGs using fresh clone-specific Airflow metadata. Host WSL/Java/Node/Edge, the Kafka broker and the Airflow tool installation were reused explicitly. The linked report records the exact portability patch applied to its baseline commit. See that report and retained command logs for the tested candidate, commands and prerequisites.

The JSON index records SHA-256 and sizes of the evidence files, exact scheduler task states, run/group IDs, model/source/split hashes and output CSV hashes. Raw source data, saved models, service metadata and run ledgers stay ignored by Git; recreate them using the documented commands. Recovery transcripts and test logs needed for assessment are retained in the repository.

See [reproduction commands](../../docs/VERIFICATION.md). Delivery remains at-least-once: the tested ack-before-ledger crash can produce two physical messages with one stable prediction ID. Separate groups may also emit the same prediction; downstream sinks should upsert by prediction_id.
