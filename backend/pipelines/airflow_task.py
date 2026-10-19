"""One isolated ML task, executed by Airflow in the pinned ML environment."""
import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

if __name__ == "__main__":
    from opsyntra.airflow_workflow import WorkflowContext, execute_task, TASKS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", choices=TASKS, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    context = WorkflowContext(ROOT, os.environ["OPSYNTRA_RUN_ID"], os.environ.get("OPSYNTRA_ENGINE", "sklearn"),
                              os.environ.get("OPSYNTRA_PREDICTION_INPUT"))
    execute_task(args.task, context, int(os.environ.get("OPSYNTRA_TASK_TRY", "1")))
