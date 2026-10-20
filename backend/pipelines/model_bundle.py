"""Export or verify a labelled saved-model bundle; no dataset or MLflow database."""
import argparse
import hashlib
import json
import sys
from pathlib import Path, PurePosixPath
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
REPORTS = {"model_training_report.json", "model_selection.md", "model_comparison.csv", "spark_training_report.json",
           "engine_comparison.json", "engine_comparison.md", "engine_validation_comparison.csv"}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def valid_member(name):
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name or ":" in name or str(path) != name:
        return False
    return (name in {"artifacts/models/best_pipeline.joblib", "artifacts/models/model_metadata.json", "artifacts/spark/selected_model.json"}
            or name.startswith("artifacts/spark/runs/") or (path.parent == PurePosixPath("reports") and path.name in REPORTS))


def verify(path):
    with ZipFile(path) as archive:
        manifest = json.loads(archive.read("bundle_manifest.json"))
        if manifest["format_version"] != 1:
            raise ValueError("Unsupported bundle format")
        members = manifest["members"]
        names = [m["path"] for m in members]
        if (not names or len({n.casefold() for n in names}) != len(names) or not all(valid_member(n) for n in names)
                or len(archive.namelist()) != len(names) + 1 or set(archive.namelist()) != set(names) | {"bundle_manifest.json"}):
            raise ValueError("Unexpected or unsafe bundle entries")
        for member in members:
            data = archive.read(member["path"])
            if len(data) != member["bytes"] or sha(data) != member["sha256"]:
                raise ValueError("Bundle checksum mismatch: " + member["path"])
    return {"status": "PASS", "bundle_sha256": sha(Path(path).read_bytes()), "manifest": manifest}


def export(backend, output):
    from opsyntra.orchestration_models import filesystem_path
    backend, output = Path(backend).resolve(), Path(output).resolve()
    if output.suffix != ".zip" or output.exists():
        raise ValueError("Choose a new .zip output; existing bundles are preserved")
    training = json.loads((backend / "reports/model_training_report.json").read_text())
    pointer = json.loads((backend / "artifacts/spark/selected_model.json").read_text())
    pipeline = (backend / pointer["pipeline_path"]).resolve()
    if not pipeline.is_relative_to(backend / "artifacts/spark/runs"):
        raise ValueError("Selected Spark pipeline is outside its artifact directory")
    paths = [backend / "artifacts/models/best_pipeline.joblib", backend / "artifacts/models/model_metadata.json",
             backend / "artifacts/spark/selected_model.json"]
    paths += [p for p in filesystem_path(pipeline).rglob("*") if p.is_file()]
    paths += [backend / "reports" / name for name in sorted(REPORTS) if (backend / "reports" / name).exists()]
    content = []
    extended_backend = filesystem_path(backend)
    for path in paths:
        path = filesystem_path(path)
        name = path.relative_to(extended_backend).as_posix()
        if not valid_member(name):
            raise ValueError("Unexpected model bundle path: " + name)
        content.append((name, path.read_bytes()))
    manifest = {"format_version": 1, "purpose": "Optional saved-model setup; not proof of fresh training",
                "dataset_sha256": training["provenance"]["source_sha256"],
                "membership_sha256": training["provenance"]["membership_sha256"],
                "sklearn_model_version": training["selected_model_version"], "spark_model_version": pointer["model_version"],
                "contains_dataset": False, "contains_mlflow_database": False,
                "members": [{"path": name, "bytes": len(data), "sha256": sha(data)} for name, data in content]}
    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, "x", compression=ZIP_DEFLATED) as archive:
        for name, data in content:
            archive.writestr(name, data)
        archive.writestr("bundle_manifest.json", json.dumps(manifest, indent=2) + "\n")
    verified = verify(output)
    output.with_suffix(".zip.sha256").write_text(verified["bundle_sha256"] + "  " + output.name + "\n", encoding="utf-8")
    return verified


def restore(backend, bundle):
    """Validate inputs before writing; extended paths support native Spark files."""
    from opsyntra.config import load_config
    from opsyntra.orchestration_models import filesystem_path
    backend = Path(backend).resolve()
    verified = verify(bundle)
    config = load_config(backend / "config.yaml")
    manifest = verified["manifest"]
    if (sha(config.raw_path.read_bytes()) != manifest["dataset_sha256"]
            or sha((config.output_dir / "memberships.csv").read_bytes()) != manifest["membership_sha256"]):
        raise ValueError("Bundle dataset/membership hashes differ; import the matching archive first")
    with ZipFile(bundle) as archive:
        payloads = [(member["path"], archive.read(member["path"])) for member in manifest["members"]]
    # Refuse a mixed model installation. Identical existing files permit recovery
    # from an interrupted restore. Named report outputs are explicitly replaced.
    for name, data in payloads:
        target = filesystem_path(backend / name)
        if name.startswith("artifacts/") and target.exists() and target.read_bytes() != data:
            raise ValueError("Different model artifacts already exist; choose a separate checkout")
    for name, data in payloads:
        target = filesystem_path(backend / name)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".restore-tmp")
        temporary.write_bytes(data)
        temporary.replace(target)
    return {**verified, "restored_to": str(backend), "dataset_and_memberships_match": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--export", type=Path)
    action.add_argument("--verify", type=Path)
    action.add_argument("--restore", type=Path, help="Restore verified models/reports into --backend after matching data import")
    parser.add_argument("--backend", type=Path, default=ROOT)
    args = parser.parse_args()
    result = (restore(args.backend, args.restore) if args.restore else
              verify(args.verify) if args.verify else export(args.backend, args.export))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
