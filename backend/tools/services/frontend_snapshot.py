"""Refresh the UI's labelled offline snapshot from the actual project outputs."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / "src"))

from opsyntra.api.app import read_overview


def main():
    path = WORKSPACE_ROOT / "frontend/src/lib/project-snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(read_overview(), indent=2) + "\n", encoding="utf-8")
    print(f"Updated real-data frontend snapshot: {path}")


if __name__ == "__main__":
    main()
