"""Bonus scheduled batch and managed streaming Kafka workflows."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator

BACKEND = Path(__file__).resolve().parents[1]


def task(name, kind):
    return BashOperator(task_id=name, cwd=str(BACKEND),
        bash_command=f'exec "$OPSYNTRA_ML_PYTHON" -m pipelines.kafka_airflow_task --task {name}',
        env={"OPSYNTRA_ML_PYTHON": os.environ.get("OPSYNTRA_ML_PYTHON", str(BACKEND / ".venv-spark/bin/python")),
             "OPSYNTRA_RUN_ID": "{{ run_id }}", "OPSYNTRA_ENGINE": "{{ params.engine }}",
             "OPSYNTRA_KAFKA_KIND": kind, "OPSYNTRA_STREAM_SECONDS": "{{ params.duration_seconds }}",
             "OPSYNTRA_DEMO_EVENTS": "{{ params.demo_events }}", "OPSYNTRA_EVENTS_PER_SEC": "{{ params.events_per_sec }}"},
        append_env=True, do_xcom_push=False)


def workflow(kind):
    return DAG(dag_id="opsyntra_kafka_" + kind, schedule="@daily" if kind == "batch" else None,
        start_date=datetime(2026, 10, 19, tzinfo=timezone.utc), catchup=False,
        is_paused_upon_creation=True, max_active_runs=1, max_active_tasks=2,
        dagrun_timeout=timedelta(hours=2),
        default_args={"owner": "opsyntra", "retries": 2, "retry_delay": timedelta(seconds=10),
                      "execution_timeout": timedelta(minutes=70)},
        params={"engine": Param("sklearn", enum=["sklearn", "spark"]),
                "duration_seconds": Param(60, type="integer", minimum=5, maximum=3600),
                "demo_events": Param(0, type="integer", minimum=0, maximum=10000),
                "events_per_sec": Param(5, type="number", exclusiveMinimum=0, maximum=1000)},
        tags=["opsyntra", "kafka", "bonus", kind],
        doc_md="""Run-specific frozen source/model, acknowledged publication, manual input commits and persistent recovery.
Batch is scheduled daily after explicitly unpausing (no catchup). Streaming owns its consumer inside the task;
the task stays RUNNING until its configured session duration or stop.requested file causes a graceful stop.
No detached daemon survives a task. Per-run group IDs plus max_active_runs=1 prevent overlapping consumers.
Retry uses the same group/ledger. Optional demo_events publishes bounded samples in a parallel task.
LocalExecutor/shared filesystem only; delivery remains at-least-once. Inspect artifacts/kafka/airflow/.""")


with workflow("batch") as batch_dag:
    task("health_checks", "batch") >> task("publish_batch", "batch") >> task("predict_batch", "batch") >> task("summarize", "batch")

with workflow("streaming") as streaming_dag:
    health = task("health_checks", "streaming")
    consumer = task("start_and_monitor_consumer", "streaming")
    samples = task("publish_samples", "streaming")
    health >> [consumer, samples]
    [consumer, samples] >> task("summarize", "streaming")
