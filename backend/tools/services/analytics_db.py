"""Apply analytics migrations or inspect storage without logging credentials."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from opsyntra.analytics.config import AnalyticsConfig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=("migrate", "status"))
    args = parser.parse_args()
    config = AnalyticsConfig.from_env()
    with config.connect("migration" if args.task == "migrate" else "read") as connection:
        if args.task == "migrate":
            from opsyntra.analytics.migrations import migrate
            result = migrate(connection)
        else:
            result = {"database": connection.info.dbname, "server_version": connection.info.server_version,
                      "timezone": connection.execute("SHOW TIME ZONE").fetchone()["TimeZone"],
                      "metrics": connection.execute("SELECT * FROM analytics.prediction_metrics_by_model").fetchall()}
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
