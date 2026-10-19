"""Load data pipeline settings; relative paths belong to the config directory."""

from dataclasses import dataclass
from math import isclose, isfinite
from pathlib import Path

import yaml


@dataclass(frozen=True)
class DataConfig:
    raw_path: Path
    archive_member: str
    normalized_path: Path
    output_dir: Path
    report_path: Path
    seed: int
    train_fraction: float
    validation_fraction: float
    test_fraction: float


@dataclass(frozen=True)
class TrainingConfig:
    models: tuple[str, ...]
    seed: int
    cv_folds: int
    selection_metric: str
    estimator_jobs: int
    artifact_dir: Path
    report_path: Path
    comparison_path: Path
    param_grids: dict


def load_config(path: Path) -> DataConfig:
    path = Path(path).resolve()
    settings = yaml.safe_load(path.read_text(encoding="utf-8"))
    data = settings["data"]
    split = settings["splitting"]
    if data["identifier"] != "customerID" or data["target"] != "Churn":
        raise ValueError("The Telco contract requires customerID and Churn.")
    policy = settings["preprocessing"]["total_charges_missing_policy"]
    if policy != "preserve_for_training_imputer":
        raise ValueError("Unsupported TotalCharges missing-value policy.")
    fractions = [float(split[key]) for key in ("train", "validation", "test")]
    if not all(isfinite(value) and 0 < value < 1 for value in fractions):
        raise ValueError("Split fractions must be finite and between zero and one.")
    if not isclose(sum(fractions), 1.0, abs_tol=1e-9):
        raise ValueError("Split fractions must sum to one.")
    seed = split["seed"]
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed < 2**32:
        raise ValueError("Seed must be an integer in [0, 2**32).")

    def resolve(value: str) -> Path:
        return (path.parent / value).resolve()

    config = DataConfig(
        raw_path=resolve(data["raw_path"]),
        archive_member=data["archive_member"],
        normalized_path=resolve(data["normalized_path"]),
        output_dir=resolve(split["output_dir"]),
        report_path=resolve(settings["report_path"]),
        seed=seed,
        train_fraction=fractions[0],
        validation_fraction=fractions[1],
        test_fraction=fractions[2],
    )
    outputs = [config.normalized_path, config.report_path] + [
        config.output_dir / name
        for name in ("memberships.csv", "manifest.json", "train.csv", "validation.csv", "test.csv")
    ]
    if config.raw_path in outputs or len(outputs) != len(set(outputs)):
        raise ValueError("Raw input and output file paths must be distinct.")
    return config


def load_training_config(path: Path) -> TrainingConfig:
    path = Path(path).resolve()
    settings = yaml.safe_load(path.read_text(encoding="utf-8"))["training"]
    models = tuple(settings["models"])
    allowed = {"dummy", "logistic_regression", "random_forest", "xgboost"}
    if not models or len(set(models)) != len(models) or not set(models) <= allowed:
        raise ValueError("Configure unique model names from dummy, logistic_regression, random_forest, xgboost.")
    for key, minimum in (("cv_folds", 2), ("estimator_jobs", 1), ("seed", 0)):
        value = settings[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"training.{key} must be an integer >= {minimum}.")
    if settings["seed"] >= 2**32:
        raise ValueError("Training seed must be less than 2**32.")
    if settings["selection_metric"] not in {"average_precision", "roc_auc", "f1", "recall"}:
        raise ValueError("Unsupported model selection metric.")
    grids = settings.get("param_grids", {})
    if not isinstance(grids, dict):
        raise ValueError("training.param_grids must be a mapping.")
    for model in models:
        grid = grids.get(model, {})
        if not isinstance(grid, dict) or any(not isinstance(values, list) or not values for values in grid.values()):
            raise ValueError(f"{model} grid values must be nonempty lists.")
    return TrainingConfig(
        models=models, seed=settings["seed"], cv_folds=settings["cv_folds"],
        selection_metric=settings["selection_metric"], estimator_jobs=settings["estimator_jobs"],
        artifact_dir=(path.parent / settings["artifact_dir"]).resolve(),
        report_path=(path.parent / settings["report_path"]).resolve(),
        comparison_path=(path.parent / settings["comparison_path"]).resolve(),
        param_grids=grids,
    )
