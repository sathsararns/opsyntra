"""Regenerate comparison evidence from saved Spark and sklearn artifacts."""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    args = parser.parse_args()
    from opsyntra.spark.training import load_spark_settings
    from opsyntra.data.training import load_training_partitions
    from opsyntra.ml.comparison import compare_engines
    settings = load_spark_settings(args.config)
    report = json.loads((args.config.resolve().parent / settings["report_path"]).read_text(encoding="utf-8"))
    partitions, _ = load_training_partitions(args.config)
    compare_engines(args.config, report, partitions)
    print("Saved engine comparison report.")
