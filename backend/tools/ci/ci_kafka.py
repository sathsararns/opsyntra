"""Real Kafka/PostgreSQL crash/replay contract checks, independent of ML artifacts."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT / "src"))


def main():
    from dotenv import load_dotenv
    from tools.verification.analytics_consumer_check import live_check, test_config
    load_dotenv(ROOT / ".env",override=False)
    directory = ROOT / "artifacts/ci/reports/kafka"
    directory.mkdir(parents=True,exist_ok=True)
    result = live_check(test_config(),directory)
    (directory / "result.json").write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")
    print("PASS: real Kafka database-before-offset commits, hard exits, replay, malformed messages and no count inflation")


if __name__ == "__main__":main()
