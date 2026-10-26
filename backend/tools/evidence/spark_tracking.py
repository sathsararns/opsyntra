"""Log the completed Spark batch from the host owning the MLflow artifact store."""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    args = parser.parse_args()
    from opsyntra.spark_tracking import log_spark_runs
    print(log_spark_runs(args.config))
