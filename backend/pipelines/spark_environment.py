"""Create a fresh Linux-native Spark venv and expose the existing project path."""
import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def create(backend):
    if os.name == "nt":
        raise ValueError("Run this setup in Ubuntu WSL")
    backend = Path(backend).resolve()
    local = backend / ".venv-spark"
    if local.exists():
        return {"status": "existing", "project_path": str(local), "runtime_path": str(local.resolve()),
                "note": "Existing environments are preserved; no automatic migration"}
    if local.is_symlink():
        raise ValueError("The Spark environment link is broken; inspect its target before recreating it")
    identity = hashlib.sha256(str(backend).encode()).hexdigest()[:12]
    target = Path.home() / ".local/share/opsyntra" / ("spark-venv-" + identity)
    if target.exists():
        raise ValueError("Native runtime already exists without its project link; inspect before reusing it")
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([sys.executable, "-m", "venv", "--copies", str(target)], check=True)
    local.symlink_to(target, target_is_directory=True)
    return {"status": "created", "project_path": str(local), "runtime_path": str(target),
            "include_system_site_packages": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", type=Path, default=ROOT)
    print(json.dumps(create(parser.parse_args().backend), indent=2))
