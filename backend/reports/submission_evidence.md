# Opsyntra verification and evidence

Generated: 2026-10-19T15:39:56.185348+00:00

Host: 67 passed, 4 Spark checks skipped. Native WSL Spark: 4 passed, no skips. All 71 unique tests passed across the two runtimes.

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

## Real bonus scheduler runs

- **batch**: `manual__opsyntra_kafka_batch_20261019T152734317462Z`; all four tasks successful; 7043 confirmed prediction rows; model `d9a13b4cf6839ed4`.
- **streaming**: `manual__opsyntra_kafka_streaming_20261019T152921722006Z`; all four tasks successful; 20 confirmed prediction rows; model `d9a13b4cf6839ed4`.

The JSON index records SHA-256 and sizes of the evidence files, exact scheduler task states, run/group IDs, model/source/split hashes and output CSV hashes. Raw source data, saved models, service metadata and run ledgers stay ignored by Git; recreate them using the documented commands. Recovery transcripts and test logs needed for assessment are retained in the repository.

See [reproduction commands](../../docs/VERIFICATION.md). Delivery remains at-least-once: the tested ack-before-ledger crash can produce two physical messages with one stable prediction ID. Separate groups may also emit the same prediction; downstream sinks should upsert by prediction_id.
