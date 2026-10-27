"""Verify data artifacts and reuse saved customer memberships for both engines."""

import json
from pathlib import Path

import pandas as pd

from opsyntra.config import load_config
from opsyntra.data.pipeline import sha256_file
from opsyntra.data.splitting import apply_memberships
from opsyntra.data.validation import TARGET


def load_training_partitions(config_path: Path):
    config = load_config(config_path)
    manifest_path = config.output_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError("Run python -m pipelines.data_pipeline first.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = [
        (config.raw_path, manifest["source_sha256"]),
        (config.normalized_path, manifest["normalized_sha256"]),
        (config.output_dir / "memberships.csv", manifest["membership_sha256"]),
    ]
    for path, digest in expected:
        if sha256_file(path) != digest:
            raise ValueError(f"Data checksum mismatch: {path}. Regenerate and review Step 1 artifacts.")
    data = pd.read_csv(config.normalized_path, dtype="string", keep_default_na=False)
    memberships = pd.read_csv(config.output_dir / "memberships.csv", dtype="string")
    if not data[TARGET].isin(["No", "Yes"]).all():
        raise ValueError("Training labels must be No or Yes.")
    partitions = apply_memberships(data, memberships)
    for name, part in partitions.items():
        if len(part) != manifest["splits"][name]["rows"]:
            raise ValueError(f"{name}: membership count differs from the saved manifest.")
    return partitions, manifest

