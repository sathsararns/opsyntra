"""CLI: train and compare three complete Spark pipelines on saved memberships."""

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--skip-tracking", action="store_true", help="Windows launcher logs artifacts from the host after WSL training.")
    args = parser.parse_args()
    if sys.platform == "win32":
        parser.error("Use ..\\scripts\\windows\\start-spark.ps1 -Task train from backend, or .\\scripts\\windows\\start-spark.ps1 -Task train from the repository root.")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from opsyntra.spark.training import run_spark_training
    run_spark_training(args.config, track=not args.skip_tracking)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
