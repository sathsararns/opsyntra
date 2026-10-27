"""Run with python -m pipelines.data_pipeline from a source checkout."""

import argparse
import logging
import sys
from pathlib import Path

# Support checkout execution before an editable package installation.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from opsyntra.data.pipeline import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Telco data and persist reproducible customer splits.")
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--archive", type=Path, help="Optional ZIP to import the configured raw CSV member.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run_pipeline(args.config, args.archive)
    except (ValueError, OSError, KeyError) as error:
        logging.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
