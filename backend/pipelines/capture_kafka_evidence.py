"""Archive actual diagnostic CLI output and existing hard-crash recovery logs."""
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT.parent / "docs/evidence"


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    captures = []
    for topic, count in (("raw", 2), ("predictions", 3), ("deadletter", 3)):
        command = [sys.executable, "-m", "pipelines.kafka_messages", "--topic", topic, "--max-messages", str(count)]
        process = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=45)
        if process.returncode:
            raise RuntimeError(process.stderr)
        value = json.loads(process.stdout)
        if not value["messages"]:
            raise ValueError("No actual messages available for " + topic)
        label = "python backend/pipelines/kafka_messages.py --topic " + topic + " --max-messages " + str(count)
        text = "$ " + label + "\n\n" + process.stdout
        path = OUTPUT / ("kafka-" + topic + ".txt")
        path.write_text(text, encoding="utf-8")
        captures.append({"kind": topic, "command": label, "file": path.name, "topic": value["topic"],
                         "note": "Read-only historical samples, not necessarily latest; application offsets unchanged"})
    report = json.loads((ROOT / "reports/kafka_consumption_report.json").read_text())
    source = ROOT / report["check_directory"]
    names = ["crash_after_ledger_commit", "restart_after_ledger_commit", "crash_after_output_ack", "restart_after_output_ack"]
    for name in names:
        original = source / (name + ".log")
        shutil.copyfile(original, OUTPUT / (name + ".log"))
        captures.append({"kind": "recovery", "file": name + ".log", "source": original.relative_to(ROOT.parent).as_posix(),
                         "result": report["calls"][name]})
    (OUTPUT / "kafka-capture.json").write_text(json.dumps({"captured_at": datetime.now(timezone.utc).isoformat(),
        "source": "Real local Kafka CLI and recorded hard-process-exit checks", "captures": captures}, indent=2) + "\n", encoding="utf-8")
    print("Saved real Kafka CLI transcripts and recovery logs:", OUTPUT)


if __name__ == "__main__":
    main()
