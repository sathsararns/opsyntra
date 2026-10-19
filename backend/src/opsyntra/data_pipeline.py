"""Run validation, deterministic normalization and reproducible split generation."""

import hashlib
import json
import logging
import platform
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd
import sklearn
import yaml

from .config import load_config
from .data_splitting import apply_memberships, create_memberships
from .data_validation import DataValidationError, TARGET, read_raw_data, validate_and_normalize

logger = logging.getLogger(__name__)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def import_archive(archive: Path, member: str, destination: Path) -> None:
    """Copy only the configured CSV member and preserve an existing raw file."""
    with ZipFile(archive) as source:
        content = source.read(member)
    if destination.exists():
        if destination.read_bytes() != content:
            raise ValueError("The existing raw CSV differs from the archive; it was left unchanged.")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as output:
        output.write(content)


def run_pipeline(config_path: Path, archive: Path | None = None) -> dict:
    config = load_config(config_path)
    if archive is not None:
        import_archive(Path(archive), config.archive_member, config.raw_path)
    if not config.raw_path.is_file():
        raise FileNotFoundError(f"Raw CSV not found: {config.raw_path}. Supply --archive for the initial import.")
    source_hash = sha256_file(config.raw_path)
    logger.info("Validating %s", config.raw_path)
    try:
        data, report = validate_and_normalize(read_raw_data(config.raw_path))
    except DataValidationError as error:
        error.report["source_sha256"] = source_hash
        write_json(config.report_path, error.report)
        raise
    memberships = create_memberships(
        data, seed=config.seed, train=config.train_fraction,
        validation=config.validation_fraction, test=config.test_fraction,
    )
    partitions = apply_memberships(data, memberships)
    customer_sets = [set(partition["customerID"]) for partition in partitions.values()]
    no_overlap = all(
        not left.intersection(right)
        for index, left in enumerate(customer_sets)
        for right in customer_sets[index + 1:]
    )
    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.normalized_path.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(config.normalized_path, index=False, lineterminator="\n")
    membership_path = config.output_dir / "memberships.csv"
    memberships.to_csv(membership_path, index=False, lineterminator="\n")
    summary = {}
    for name, partition in partitions.items():
        path = config.output_dir / f"{name}.csv"
        partition.to_csv(path, index=False, lineterminator="\n")
        counts = partition[TARGET].value_counts()
        summary[name] = {
            "rows": len(partition),
            "target_counts": {label: int(counts.get(label, 0)) for label in ("No", "Yes")},
            "churn_percentage": round(float(partition[TARGET].eq("Yes").mean()) * 100, 4),
            "missing_total_charges": int(partition["TotalCharges"].isna().sum()),
            "sha256": sha256_file(path),
        }
        logger.info("%s: %s customers, %.4f%% churn", name, len(partition), summary[name]["churn_percentage"])
    manifest = {
        "schema_version": 1,
        "source_file": config.raw_path.name,
        "source_sha256": source_hash,
        "normalized_sha256": sha256_file(config.normalized_path),
        "membership_sha256": sha256_file(membership_path),
        "seed": config.seed,
        "requested_fractions": {"train": config.train_fraction, "validation": config.validation_fraction, "test": config.test_fraction},
        "method": "Sort by customerID; two-stage sklearn stratified train_test_split; persist ID membership for both engines.",
        "runtime": {"python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__, "scikit_learn": sklearn.__version__, "PyYAML": yaml.__version__},
        "splits": summary,
        "checks": {"all_customers_assigned_once": len(memberships) == len(data) and memberships["customerID"].is_unique, "no_partition_overlap": no_overlap, "no_learned_preprocessing_fitted": True},
    }
    write_json(config.output_dir / "manifest.json", manifest)
    report.update({"source_sha256": source_hash, "splits": summary, "split_checks": manifest["checks"], "membership_sha256": manifest["membership_sha256"]})
    write_json(config.report_path, report)
    logger.info("Validation %s; report: %s", report["status"], config.report_path)
    return report
