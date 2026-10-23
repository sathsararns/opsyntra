"""Test-only API entrypoint using a fresh synthetic model and genuine HTTP routes."""
import argparse
import json
import sys
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main():
    import uvicorn
    import opsyntra.web_api as api
    from opsyntra.model_inference import load_pipeline
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    root = args.fixture.resolve()
    manifest = json.loads((root / "ci_fixture.json").read_text())
    if manifest.get("synthetic") is not True: raise ValueError("CI fixture API requires explicitly synthetic artifacts")
    original_overview = api.read_overview
    def overview():
        return {**original_overview(root / "config.yaml"), "ci_fixture":True}
    @lru_cache(maxsize=1)
    def model(): return load_pipeline(root / "artifacts/models/best_pipeline.joblib")
    api.read_overview = overview
    api.get_model = model
    uvicorn.run(api.app, host="127.0.0.1", port=args.port)


if __name__ == "__main__": main()
