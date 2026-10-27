"""Prepare synthetic CI data/models or explicitly verified full-data artifacts."""
import argparse
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))
DATASET_SHA256 = "88be4b93fbe0cc83421af1c503794c97c342eca914c1576db7c276e61d61358a"


def prepare(directory, *, full_data=False):
    import yaml
    from opsyntra.data.pipeline import run_pipeline
    from opsyntra.ml.training import run_training
    from tests.helpers import sample_data
    directory = Path(directory).resolve()
    if directory == ROOT and not (full_data and os.environ.get("GITHUB_ACTIONS") == "true"):
        raise ValueError("Use a separate fixture directory; the application backend is protected")
    if not full_data and directory.exists() and any(directory.iterdir()):
        raise ValueError("Synthetic fixture destination must be empty")
    directory.mkdir(parents=True, exist_ok=True)
    settings = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    raw = directory / settings["data"]["raw_path"]
    raw.parent.mkdir(parents=True, exist_ok=True)
    if full_data:
        url = os.environ.get("OPSYNTRA_CI_DATASET_URL", "")
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("Provide a public HTTPS URL to the checksum-exact CSV (no embedded credentials)")
        if raw.exists(): raise ValueError("Full-data destination already contains raw data")
        with urlopen(url, timeout=60) as response:
            if urlsplit(response.geturl()).scheme != "https": raise ValueError("Dataset redirects must stay on HTTPS")
            content = response.read(20_000_001)
        if len(content) > 20_000_000 or hashlib.sha256(content).hexdigest() != DATASET_SHA256:
            raise ValueError("Full-data download does not match the required Telco CSV checksum")
        raw.write_bytes(content)
    else:
        sample_data(120).to_csv(raw, index=False, lineterminator="\n")
        settings["training"].update(cv_folds=2, estimator_jobs=1,
            param_grids={"dummy":{},"logistic_regression":{"classifier__C":[1.]},
                        "random_forest":{"classifier__n_estimators":[5]},"xgboost":{"classifier__n_estimators":[5]}})
        (directory / "config.yaml").write_text(yaml.safe_dump(settings), encoding="utf-8")
        shutil.copytree(ROOT / "examples", directory / "examples")
    run_pipeline(directory / "config.yaml")
    report = run_training(directory / "config.yaml")
    from opsyntra.api.app import read_overview
    overview = read_overview(directory / "config.yaml")
    manifest = {"synthetic":not full_data,"customers":overview["dataset"]["customers"],
                "source_sha256":hashlib.sha256(raw.read_bytes()).hexdigest(),"model_version":report["selected_model_version"],
                "model":report["selected_model"],"status":"PASS"}
    (directory / "ci_fixture.json").write_text(json.dumps(manifest,indent=2)+"\n",encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--full-data", action="store_true")
    args = parser.parse_args()
    print(json.dumps(prepare(args.directory, full_data=args.full_data),indent=2))


if __name__ == "__main__": main()
