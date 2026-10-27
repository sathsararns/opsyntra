"""Publish Telco customer events: one CLI for batch and continuous streaming."""
import argparse
import json
import logging
import signal
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))


def main():
    from opsyntra.kafka.config import load_kafka_config
    from opsyntra.kafka.publishing import run_producer
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["streaming", "batch"], required=True)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--input", type=Path, help="CSV path relative to your terminal; default is the configured raw dataset")
    parser.add_argument("--bootstrap-servers", help="Override broker address")
    parser.add_argument("--events-per-sec", type=float)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--max-events", type=int, help="Optional bound for demonstrations; streaming is continuous when omitted")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--exclude-churn", action="store_true", help="Omit simulated ground-truth labels from messages")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = threading.Event()
    def request_stop(signum, frame):
        logging.info("Stop requested; finishing and confirming queued messages")
        stop.set()
    for name in ("SIGINT", "SIGTERM"):
        signal.signal(getattr(signal, name), request_stop)
    try:
        settings = load_kafka_config(args.config)
        if args.bootstrap_servers:
            settings["bootstrap_servers"] = args.bootstrap_servers
        result = run_producer(settings, mode=args.mode, input_csv=args.input, checkpoint=args.checkpoint,
            resume=args.resume, batch_size=args.batch_size, events_per_sec=args.events_per_sec,
            max_events=args.max_events, seed=args.seed, exclude_churn=args.exclude_churn, stop=stop)
        print(json.dumps(result, indent=2, allow_nan=False))
        return 0
    except Exception as error:
        logging.error("%s", error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
