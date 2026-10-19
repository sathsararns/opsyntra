"""Consume raw Telco events, publish model predictions, and recover safely."""
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
    from opsyntra.kafka_config import load_kafka_config
    from opsyntra.kafka_consumption import run_consumer
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["streaming", "batch"], required=True)
    parser.add_argument("--engine", choices=["sklearn", "spark"], default="sklearn")
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--model", type=Path, help="Complete Joblib pipeline or native Spark pipeline directory")
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--group-id", help="Stable group for restart; defaults to opsyntra-churn-<engine>")
    parser.add_argument("--state-dir", type=Path, help="Persistent recovery directory; reuse it on restart")
    parser.add_argument("--max-messages", type=int, help="Input record limit; batch defaults to configured 1000")
    parser.add_argument("--batch-size", type=int, help="Records per inference/delivery/commit group")
    parser.add_argument("--idle-timeout", type=float)
    parser.add_argument("--timeout-seconds", type=float, help="Maximum processing time for bounded consumption")
    parser.add_argument("--offset-window", type=Path, help="Batch JSON with cluster_id, topic and per-partition start/end offsets")
    parser.add_argument("--test-crash-at", choices=["after_output_ack", "after_ledger_commit"], help=argparse.SUPPRESS)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = threading.Event()
    def stop_requested(signum, frame):
        logging.info("Stop requested; finishing current output deliveries and input commits")
        stop.set()
    for name in ("SIGINT", "SIGTERM"):
        signal.signal(getattr(signal, name), stop_requested)
    def fault_hook(stage):
        if args.test_crash_at == stage:
            logging.error("TEST ONLY: hard exit at %s before input offset commit", stage)
            os._exit(86)
    try:
        settings = load_kafka_config(args.config)
        if args.bootstrap_servers:
            settings["bootstrap_servers"] = args.bootstrap_servers
        result = run_consumer(settings, mode=args.mode, engine=args.engine, group_id=args.group_id,
            model_path=args.model, state_dir=args.state_dir, max_messages=args.max_messages,
            batch_size=args.batch_size, idle_timeout=args.idle_timeout, timeout_seconds=args.timeout_seconds,
            offset_window=args.offset_window, stop=stop, fault_hook=fault_hook)
        print(json.dumps(result, indent=2, allow_nan=False))
        return 2 if result["status"] == "incomplete" else 0
    except Exception as error:
        logging.error("%s", error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
