# Core workflow commands

Run these entrypoints from `backend/`:

| Module | Purpose |
|---|---|
| `pipelines.data_pipeline` | Validate/normalize data and persist shared split memberships |
| `pipelines.training_pipeline` | Tune, train, select, assess and persist sklearn pipelines |
| `pipelines.inference_pipeline` | Predict a JSON customer or CSV batch using the saved sklearn pipeline |
| `pipelines.engine_comparison` | Regenerate sklearn/Spark comparison from saved artifacts |
| `pipelines.spark_training_pipeline` | Train and compare Spark pipelines in the Spark runtime |
| `pipelines.spark_inference_pipeline` | Predict raw records using the saved Spark PipelineModel |
| `pipelines.airflow_task` | Execute one task of the required eight-task ML DAG |
| `pipelines.kafka_airflow_task` | Execute one task of the bonus batch/streaming Kafka DAGs |

Business logic lives under `src/opsyntra/`. Service management, checks, CI
fixtures and evidence utilities live under [tools/](../tools/README.md).
