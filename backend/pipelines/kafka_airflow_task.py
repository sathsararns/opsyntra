"""Small process boundary for bonus Kafka Airflow tasks; no dataset XComs."""
import argparse
import json
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main():
    from opsyntra.orchestration.kafka import KafkaWorkflow
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", required=True, choices=["health_checks", "publish_batch", "predict_batch", "publish_samples", "start_and_monitor_consumer", "summarize"])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    workflow = KafkaWorkflow(ROOT, os.environ["OPSYNTRA_KAFKA_KIND"], os.environ["OPSYNTRA_RUN_ID"],
                             os.environ.get("OPSYNTRA_ENGINE", "sklearn"),
                             int(os.environ.get("OPSYNTRA_STREAM_SECONDS", "60")),
                             int(os.environ.get("OPSYNTRA_DEMO_EVENTS", "0")),
                             float(os.environ.get("OPSYNTRA_EVENTS_PER_SEC", "5")))
    print(json.dumps(getattr(workflow, args.task)(), indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
