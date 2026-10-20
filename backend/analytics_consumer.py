"""Consume existing Kafka customer/prediction/error events into PostgreSQL."""
import argparse
import json
import logging
import os
import signal
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))


def main():
    from dotenv import load_dotenv
    from opsyntra.analytics.config import AnalyticsConfig
    from opsyntra.analytics.consumer import Options, run
    from opsyntra.kafka_config import atomic_json, load_kafka_config
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("streaming", "batch"), default="streaming")
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--group-id")
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--raw-topic")
    parser.add_argument("--prediction-topic")
    parser.add_argument("--deadletter-topic")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--snapshot", action="store_true", help="Batch: capture all topic end offsets and finish that window")
    parser.add_argument("--timeout-seconds", type=float, help="Streaming duration or batch completion deadline")
    parser.add_argument("--idle-timeout", type=float, default=30)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=.5)
    parser.add_argument("--max-event-bytes", type=int, default=262144)
    parser.add_argument("--report", type=Path, help="Optional credential-free JSON run summary")
    parser.add_argument("--test-crash-at", choices=("before_db_commit", "after_db_commit", "after_kafka_commit"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    load_dotenv(args.env_file, override=False)
    args.group_id = args.group_id or os.getenv("OPSYNTRA_ANALYTICS_GROUP_ID", "opsyntra-analytics-v1")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stop = threading.Event()
    def shutdown(signum, frame): stop.set()
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name): signal.signal(getattr(signal, name), shutdown)
    def fault(stage):
        if stage == args.test_crash_at:
            logging.error(json.dumps({"event": "test_hard_exit", "stage": stage}))
            os._exit(86)
    try:
        config = AnalyticsConfig.from_env(env_file=args.env_file, required_roles=("write",))
        settings = load_kafka_config(args.config)
        if args.bootstrap_servers: settings["bootstrap_servers"] = args.bootstrap_servers
        for key, value in (("raw", args.raw_topic), ("predictions", args.prediction_topic), ("deadletter", args.deadletter_topic)):
            if value: settings["topics"][key] = value
        limit = args.max_messages
        if args.mode == "batch" and not args.snapshot and limit is None: limit = 1000
        options = Options(group_id=args.group_id, mode=args.mode, batch_size=args.batch_size, max_messages=limit,
            snapshot=args.snapshot, timeout_seconds=args.timeout_seconds if args.timeout_seconds is not None else (120 if args.mode == "batch" else None),
            idle_timeout=args.idle_timeout, retries=args.retries, retry_delay=args.retry_delay, max_event_bytes=args.max_event_bytes)
        result = run(settings, lambda: config.connect("write"), options=options, stop=stop, fault_hook=fault)
        if args.report: atomic_json(args.report, result)
        print(json.dumps(result, indent=2))
        return 2 if result["status"] == "incomplete" else 0
    except Exception as error:
        # Persist only type, never DSNs or payloads, in public logs/reports.
        result = {"status": "failed", "error_type": type(error).__name__, "group_id": args.group_id,
                  "message": "Analytics ingestion stopped; inspect readiness/configuration and rerun the same group for safe replay"}
        from confluent_kafka import KafkaException
        if isinstance(error, KafkaException) and error.args:
            result["kafka_error_code"] = error.args[0].code()
        if args.report: atomic_json(args.report, result)
        logging.error(json.dumps(result))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
