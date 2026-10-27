"""Start the localhost MLflow UI using the same store as training."""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from opsyntra.config import load_training_config
from opsyntra.ml.tracking import tracking_uri


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()
    config = load_training_config(args.config)
    return subprocess.call([
        sys.executable, "-m", "mlflow", "server", "--backend-store-uri", tracking_uri(config),
        "--default-artifact-root", config.tracking_artifacts.as_uri(),
        "--host", "127.0.0.1", "--port", str(args.port), "--workers", "1",
    ], cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
