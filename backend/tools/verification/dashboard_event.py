"""Browser-check helper: publish through the real producer, independently check SQL."""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime,timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"src"))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task",choices=("publish","check-sql"))
    parser.add_argument("--manifest",type=Path,required=True)
    parser.add_argument("--summary",type=Path)
    args=parser.parse_args()
    manifest=json.loads(args.manifest.read_text())
    if args.task=="publish":
        result=subprocess.run([sys.executable,str(ROOT/"producer.py"),"--mode","streaming","--max-events","1","--events-per-sec","1","--exclude-churn","--config",manifest["config"],"--input",manifest["csv"]],text=True,capture_output=True,timeout=30,check=True)
        Path(manifest["directory"],"producer.txt").write_text(result.stdout+result.stderr,encoding="utf-8")
        from confluent_kafka import Consumer,TopicPartition
        client=Consumer({"bootstrap.servers":manifest["bootstrap_servers"],"group.id":manifest["group"]+"-observer","enable.auto.commit":False})
        try:
            client.assign([TopicPartition(manifest["topics"]["raw"],0,0)])
            message=client.poll(10)
            if message is None or message.error(): raise RuntimeError("Published raw event not available")
            event=json.loads(message.value())
        finally: client.close()
        print(json.dumps({"event_id":event["event_id"],"customer_id":event["customerID"],"published_at":datetime.now(timezone.utc).isoformat()}))
    else:
        from opsyntra.analytics.config import AnalyticsConfig
        summary=json.loads(args.summary.read_text())
        scope=summary["scope"]
        with AnalyticsConfig.from_env(required_roles=("read",)).connect("read") as c:
            result=c.execute("""SELECT count(*) AS prediction_volume,count(DISTINCT event_id) AS unique_events,
                count(DISTINCT customer_id) AS unique_customers,avg(churn_probability) AS mean_risk,
                avg((prediction='Yes')::int)::float8 AS predicted_churn_share
                FROM analytics.prediction_events WHERE engine=%s AND model_version=%s AND customer_id=%s
                AND event_ts >= %s AND event_ts < %s""",(scope["engine"],scope["model_version"],manifest["customer_id"],scope["start"],scope["end"])).fetchone()
        print(json.dumps(result))


if __name__=="__main__": main()
