"""An eight-task, manually triggered sklearn/Spark ML workflow."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import yaml

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator

BACKEND = Path(__file__).resolve().parents[1]
POLICY = yaml.safe_load(Path(os.environ.get("OPSYNTRA_CONFIG", BACKEND / "config.yaml")).read_text(encoding="utf-8"))["orchestration"]
TASK_IDS = ("validate_data", "prepare_splits", "tune_and_train", "evaluate", "validate_candidate",
            "save_selected_model", "predict", "save_results")

with DAG(
    dag_id=POLICY["dag_id"], description="Validate → split → tune/train → evaluate → approve → save → predict → results",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), schedule=None, catchup=False,
    max_active_runs=1, max_active_tasks=1, dagrun_timeout=timedelta(minutes=POLICY["dag_timeout_minutes"]),
    default_args={"owner": "opsyntra", "retries": POLICY["retries"], "retry_delay": timedelta(seconds=POLICY["retry_delay_seconds"]),
                  "execution_timeout": timedelta(minutes=POLICY["task_timeout_minutes"])},
    params={"engine": Param("sklearn", enum=["sklearn", "spark"], description="ML engine; both reuse saved memberships"),
            "prediction_input": Param("examples/customers.csv", type="string", description="CSV path relative to backend")},
    tags=["opsyntra", "ml", "sklearn", "spark"],
    doc_md="""Eight independently logged tasks. Full pipelines stay inside training CV folds.
Validation chooses the model/threshold and controls approval; test scores are for reporting.
Artifacts live in `backend/artifacts/airflow/runs/<engine>/<run-key>/`.
The final result pointer updates only after all eight tasks succeed. No scheduled backfills.""",
) as dag:
    previous = None
    for task_id in TASK_IDS:
        current = BashOperator(
            task_id=task_id, cwd=str(BACKEND),
            bash_command=f'exec "$OPSYNTRA_ML_PYTHON" -m pipelines.airflow_task --task {task_id}',
            env={"OPSYNTRA_ML_PYTHON": os.environ.get("OPSYNTRA_ML_PYTHON", str(BACKEND / ".venv-spark/bin/python")),
                 "OPSYNTRA_RUN_ID": "{{ run_id }}", "OPSYNTRA_ENGINE": "{{ params.engine }}",
                 "OPSYNTRA_PREDICTION_INPUT": "{{ params.prediction_input }}", "OPSYNTRA_TASK_TRY": "{{ ti.try_number }}"},
            append_env=True, do_xcom_push=False,
        )
        if previous is not None:
            previous >> current
        previous = current
