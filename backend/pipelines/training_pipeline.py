"""CLI: python -m pipelines.training_pipeline."""

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from opsyntra.ml.training import run_training


def main() -> int:
    parser = argparse.ArgumentParser(description="Train and compare complete Telco model pipelines.")
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run_training(args.config)
    except (ValueError, OSError, KeyError, ImportError) as error:
        logging.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
