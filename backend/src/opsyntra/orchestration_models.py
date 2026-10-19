"""Engine-specific train/evaluate operations for separately scheduled DAG tasks."""

import hashlib
import importlib.metadata
import json
import time
import uuid
from pathlib import Path

import joblib
import numpy as np
from sklearn.model_selection import GridSearchCV, ParameterGrid, StratifiedKFold, FixedThresholdClassifier
from sklearn.frozen import FrozenEstimator

from .data_validation import FEATURE_COLUMNS, TARGET
from .model_building import build_model_pipeline
from .model_evaluation import evaluate_probabilities, select_threshold
from .training_data import load_training_partitions


def dump_pipeline(model, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    joblib.dump(model, temporary, compress=3)
    temporary.replace(path)


def model_digest(path):
    from .data_pipeline import sha256_file
    if path.is_file():
        return sha256_file(path)
    digest = hashlib.sha256()
    files = sorted(p for p in path.rglob("*") if p.is_file())
    if not files:
        raise ValueError("Model artifact is empty.")
    for file in files:
        digest.update(file.relative_to(path).as_posix().encode())
        digest.update(file.read_bytes())
    return digest.hexdigest()


def _identity(ctx, name, parameters, threshold):
    return {"engine": ctx.engine, "model_name": name, "parameters": parameters,
            "source_sha256": ctx.snapshot["source_sha256"],
            "membership_sha256": ctx.snapshot["membership_sha256"],
            "feature_columns": list(FEATURE_COLUMNS), "target_mapping": {"No": 0, "Yes": 1},
            "fit_partition": "train", "selection_partition": "validation", "decision_threshold": threshold,
            "runtime": {p: importlib.metadata.version(p) for p in ("scikit-learn", "pandas", "numpy", "joblib")}}


def train_sklearn(ctx):
    partitions, _ = load_training_partitions(ctx.config_path)
    train = partitions["train"]
    settings = ctx.settings["training"]
    X, y = train[list(FEATURE_COLUMNS)], train[TARGET].map({"No": 0, "Yes": 1}).astype(int)
    candidates = {}
    destination = ctx.run / "training" / uuid.uuid4().hex
    folds = StratifiedKFold(n_splits=settings["cv_folds"], shuffle=True, random_state=settings["seed"])
    for name in settings["models"]:
        ctx.log.info("Training %s: %s-fold CV; no validation/test fitting", name, settings["cv_folds"])
        started = time.perf_counter()
        search = GridSearchCV(build_model_pipeline(name, seed=settings["seed"], jobs=settings["estimator_jobs"]),
                              settings["param_grids"].get(name, {}), cv=folds, scoring="average_precision",
                              n_jobs=1, refit=True, error_score="raise")
        search.fit(X, y)
        if name == "random_forest":
            search.best_estimator_.named_steps["classifier"].set_params(n_jobs=1)
        path = destination / f"{name}.joblib"
        dump_pipeline(search.best_estimator_, path)
        candidates[name] = {"parameters": search.best_params_, "path": ctx.relative(path),
                            "sha256": model_digest(path), "training_seconds": time.perf_counter() - started,
                            "cv_mean_average_precision": float(search.best_score_),
                            "cv_results": {"parameters": search.cv_results_["params"],
                                           "mean_scores": search.cv_results_["mean_test_score"].tolist()}}
    return {"engine": ctx.engine, "candidates": candidates, "fit_partition": "train", "cv_folds": settings["cv_folds"]}


def evaluate_sklearn(ctx, training):
    partitions, _ = load_training_partitions(ctx.config_path)
    train, validation = partitions["train"], partitions["validation"]
    X, y = validation[list(FEATURE_COLUMNS)], validation[TARGET].map({"No": 0, "Yes": 1}).astype(int)
    candidates, fitted = {}, {}
    destination = ctx.run / "evaluation" / uuid.uuid4().hex
    for name, result in training["candidates"].items():
        source = ctx.resolve(result["path"])
        if model_digest(source) != result["sha256"]:
            raise ValueError("Training artifact changed before evaluation.")
        model = joblib.load(source)
        probabilities = model.predict_proba(X)[:, 1]
        threshold = .5 if name == "dummy" else select_threshold(y, probabilities)["threshold"]
        if name != "dummy":
            classifier = FixedThresholdClassifier(FrozenEstimator(model.named_steps["classifier"]),
                                                   threshold=threshold, response_method="predict_proba")
            classifier.fit(model[:-1].transform(train[list(FEATURE_COLUMNS)]), train[TARGET].map({"No": 0, "Yes": 1}).astype(int))
            model.steps[-1] = ("classifier", classifier)
        identity = _identity(ctx, name, result["parameters"], threshold)
        version = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16]
        model.opsyntra_metadata_ = {**identity, "model_version": version}
        path = destination / f"{name}.joblib"
        dump_pipeline(model, path)
        reloaded = joblib.load(path)
        np.testing.assert_allclose(reloaded.predict_proba(X)[:, 1], probabilities, rtol=0, atol=1e-12)
        np.testing.assert_array_equal(reloaded.predict(X), model.predict(X))
        candidates[name] = {**result, "path": ctx.relative(path), "sha256": model_digest(path),
                            "model_version": version, "decision_threshold": threshold,
                            "validation": evaluate_probabilities(y, probabilities, threshold), "reload_matches": True}
        fitted[name] = reloaded
    selected = max(candidates, key=lambda name: candidates[name]["validation"]["average_precision"])
    test = partitions["test"]
    probabilities = fitted[selected].predict_proba(test[list(FEATURE_COLUMNS)])[:, 1]
    test_metrics = evaluate_probabilities(test[TARGET].map({"No": 0, "Yes": 1}), probabilities,
                                         candidates[selected]["decision_threshold"])
    return _evaluation_result(ctx, candidates, selected, test_metrics, y)


def _evaluation_result(ctx, candidates, selected, test_metrics, validation_labels):
    baseline = evaluate_probabilities(validation_labels, np.zeros(len(validation_labels)))["average_precision"]
    return {"engine": ctx.engine, "selected_model": selected, "candidates": candidates,
            "selected_test_metrics": test_metrics, "dummy_validation_average_precision": baseline,
            "test_used_for_selection": False, "selection_metric": "validation_average_precision",
            "threshold_metric": "validation_f1", "source_sha256": ctx.snapshot["source_sha256"],
            "membership_sha256": ctx.snapshot["membership_sha256"]}


def _spark_context(ctx):
    from .spark_runtime import create_spark
    from .spark_training import load_spark_partitions
    parts, _ = load_training_partitions(ctx.config_path)
    spark = create_spark(ctx.settings["spark"], ctx.run)
    frames = load_spark_partitions(spark, ctx.config_path, parts, uuid.uuid4().hex, ctx.settings["spark"])
    return spark, frames, parts


def train_spark(ctx):
    from pyspark.sql import functions as F
    from .spark_training import fit_candidate, collect_scores
    settings = ctx.settings["spark"]
    spark, frames, _ = _spark_context(ctx)
    candidates = {}
    destination = ctx.run / "training" / uuid.uuid4().hex
    try:
        for name, specification in settings["models"].items():
            started = time.perf_counter()
            searches = []
            for overrides in ParameterGrid(specification["grid"]):
                parameters = {**specification["parameters"], **overrides}
                scores = []
                for fold in range(settings["cv_folds"]):
                    ctx.log.info("Spark %s %s fold %s", name, overrides, fold + 1)
                    fit, _ = fit_candidate(name, parameters, frames["train"].filter(F.col("cv_fold").cast("int") != fold), settings["seed"])
                    predicted = collect_scores(fit, frames["train"].filter(F.col("cv_fold").cast("int") == fold))
                    scores.append(evaluate_probabilities(predicted.label, predicted.churn_probability)["average_precision"])
                searches.append({"parameters": parameters, "fold_scores": scores, "mean_score": float(np.mean(scores))})
            best = max(searches, key=lambda search: search["mean_score"])
            model, timing = fit_candidate(name, best["parameters"], frames["train"], settings["seed"])
            path = destination / name
            model.save(path.as_uri())
            candidates[name] = {"parameters": best["parameters"], "path": ctx.relative(path), "sha256": model_digest(path),
                                "cv_mean_average_precision": best["mean_score"], "cv_results": searches,
                                "training_seconds": time.perf_counter() - started, "final_fit_timing": timing}
        return {"engine": ctx.engine, "candidates": candidates, "fit_partition": "train", "cv_folds": settings["cv_folds"]}
    finally:
        for frame in frames.values():
            frame.unpersist()
        spark.stop()


def evaluate_spark(ctx, training):
    from pyspark.ml import PipelineModel
    from .spark_training import collect_scores
    from .spark_pipeline import SparkChurnDecision
    spark, frames, parts = _spark_context(ctx)
    candidates, models = {}, {}
    destination = ctx.run / "evaluation" / uuid.uuid4().hex
    try:
        for name, result in training["candidates"].items():
            source = ctx.resolve(result["path"])
            if model_digest(source) != result["sha256"]:
                raise ValueError("Training artifact changed before evaluation.")
            model = PipelineModel.load(source.as_uri())
            scored = collect_scores(model, frames["validation"])
            threshold = select_threshold(scored.label, scored.churn_probability)["threshold"]
            identity = _identity(ctx, name, result["parameters"], threshold)
            version = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16]
            model.stages.append(SparkChurnDecision(threshold, version))
            path = destination / name
            model.save(path.as_uri())
            reloaded = PipelineModel.load(path.as_uri())
            after = collect_scores(reloaded, frames["validation"].drop("Churn", "label"))
            np.testing.assert_allclose(after.churn_probability, scored.churn_probability, rtol=0, atol=1e-10)
            np.testing.assert_array_equal(after.prediction, np.where(scored.churn_probability >= threshold, "Yes", "No"))
            candidates[name] = {**result, "path": ctx.relative(path), "sha256": model_digest(path),
                                "model_version": version, "decision_threshold": threshold,
                                "validation": evaluate_probabilities(scored.label, scored.churn_probability, threshold), "reload_matches": True}
            models[name] = reloaded
        selected = max(candidates, key=lambda name: candidates[name]["validation"]["average_precision"])
        test = collect_scores(models[selected], frames["test"].drop("Churn", "label"))
        expected = parts["test"]
        if test.customerID.tolist() != expected.customerID.tolist():
            raise ValueError("Test customer alignment failed.")
        metrics = evaluate_probabilities(expected.Churn.map({"No": 0, "Yes": 1}), test.churn_probability,
                                         candidates[selected]["decision_threshold"])
        return _evaluation_result(ctx, candidates, selected, metrics, parts["validation"].Churn.map({"No": 0, "Yes": 1}))
    finally:
        for frame in frames.values():
            frame.unpersist()
        spark.stop()
