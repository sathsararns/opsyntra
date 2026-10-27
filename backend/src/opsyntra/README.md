# Application source

The package is grouped by responsibility. Start with `data/` and `ml/` to learn
the core ML workflow; the other folders connect it to the application.

| Folder | Files and purpose |
|---|---|
| `data/` | `validation.py` checks the Telco contract; `splitting.py` saves customer memberships; `pipeline.py` prepares artifacts; `training.py` verifies and loads partitions. |
| `ml/` | `preprocessing.py`, `building.py`, `training.py`, `evaluation.py`, `inference.py`, `tracking.py`, `comparison.py` and `notebooks.py`. |
| `spark/` | `runtime.py`, `pipeline.py`, `training.py`, `inference.py` and `tracking.py` implement the PySpark engine. |
| `kafka/` | `config.py`, `events.py`, `publishing.py`, `models.py`, `consumption.py` and `recovery.py` implement event prediction and recovery. |
| `orchestration/` | `workflow.py` implements eight ML tasks; `models.py` separates engine training/evaluation; `kafka.py` implements bonus Kafka tasks. |
| `analytics/` | PostgreSQL configuration, event contracts, storage, ingestion, consumer, migrations and read API. |
| `api/` | `app.py` connects saved-model inference and analytics to FastAPI. |
| `config.py` | Shared data/training YAML settings. |

## Reading order

1. `../../config.yaml` and `config.py`.
2. `data/validation.py`, `data/splitting.py`, `data/pipeline.py`, `data/training.py`.
3. `ml/preprocessing.py`, `ml/building.py`, `ml/evaluation.py`, `ml/training.py`, `ml/inference.py`.
4. `api/app.py`, then frontend customer and CSV components.
5. Kafka event/publishing/consumption/recovery files, followed by analytics.
6. Spark, orchestration, tracking and verification helpers.

Terminal entrypoints remain in `backend/pipelines/`. Development commands remain
in `backend/tools/`. For example, from `backend/`:

```powershell
python -m pipelines.data_pipeline
python -m pipelines.training_pipeline
python -m pipelines.inference_pipeline --record examples/customer.json
python -m uvicorn opsyntra.api.app:app --app-dir src --host 127.0.0.1 --port 8000
```

## Existing saved models

`_compat.py` lazily maps historical flat module names to the grouped modules.
It contains no duplicate application logic. Existing Joblib objects can still
resolve `opsyntra.preprocessing_sklearn.NormalizeTelcoInputs`; existing Spark
metadata can still resolve `opsyntra.spark_pipeline` custom stages. Aliases
return the same classes/modules as the new paths, without importing optional
Spark/Kafka dependencies at package startup.

New code uses paths such as `opsyntra.ml.inference` and `opsyntra.kafka.events`.
Existing data, split memberships, model bytes, versions and recovery stores are
retained. Historical reports and notebook outputs keep their original run
details. Notebook sources use the new imports; moving files does not refresh
their dated analytical results.

MLflow sklearn model exports include the complete `opsyntra` package. Spark
source artifacts retain that package hierarchy, including the compatibility
module, so downloaded models have the code needed by their persisted stages.
