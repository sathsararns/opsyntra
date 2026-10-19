"""Training-only CV, validation selection and native Spark pipeline persistence."""

import hashlib
import importlib.metadata
import xgboost
import json
import logging
import platform
import time
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from sklearn.model_selection import ParameterGrid, StratifiedKFold

from .config import load_config
from .data_pipeline import write_json, sha256_file
from .data_validation import FEATURE_COLUMNS, NUMERIC_COLUMNS
from .model_evaluation import evaluate_probabilities, select_threshold
from .training_data import load_training_partitions
from .spark_pipeline import build_spark_preprocessing, build_spark_classifier, SparkChurnDecision
from .spark_runtime import create_spark

logger = logging.getLogger(__name__)


def load_spark_settings(config_path):
    settings = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))["spark"]
    if set(settings["models"]) != {"logistic_regression", "random_forest", "xgboost"}:
        raise ValueError("Spark requires logistic_regression, random_forest and xgboost.")
    for key, minimum in (("cv_folds", 2), ("shuffle_partitions", 1), ("inference_repeats", 1), ("seed", 0)):
        value = settings[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"spark.{key} must be an integer >= {minimum}.")
    if settings["seed"] >= 2**32:
        raise ValueError("Spark seed must be less than 2**32.")
    return settings


def collect_scores(model, frame):
    """Collect probabilities, not just count(): this executes the classifier."""
    scored = model.transform(frame)
    columns = ["customerID", vector_to_array("probability")[1].alias("churn_probability")]
    if "label" in frame.columns:
        columns.append("label")
    if "prediction" in scored.columns:
        columns.extend(["prediction", "model_version"])
    rows = scored.select(*columns).collect()
    result = pd.DataFrame([row.asDict() for row in rows])
    return result.sort_values("customerID").reset_index(drop=True)


def fit_candidate(name, parameters, train, seed):
    start = time.perf_counter()
    preprocessing = build_spark_preprocessing(name).fit(train)
    transformed = preprocessing.transform(train).select("features", "label").cache()
    try:
        # Cache materialization is an action, and fitting below consumes features.
        transformed.count()
        preprocessing_seconds = time.perf_counter() - start
        start = time.perf_counter()
        classifier = build_spark_classifier(name, parameters, seed).fit(transformed)
        fit_seconds = time.perf_counter() - start
    finally:
        transformed.unpersist()
    return PipelineModel(stages=[*preprocessing.stages, classifier]), {
        "preprocessing_seconds": preprocessing_seconds, "classifier_fit_seconds": fit_seconds,
        "fit_total_seconds": preprocessing_seconds + fit_seconds,
    }


def load_spark_partitions(spark, config_path, pandas_partitions, batch_id, settings):
    config = load_config(config_path)
    raw = spark.read.option("header", True).csv(config.normalized_path.as_uri())
    members = spark.read.option("header", True).csv((config.output_dir / "memberships.csv").as_uri())
    joined = raw.join(members, "customerID", "inner").withColumn(
        "label", F.when(F.col("Churn") == "Yes", 1.).when(F.col("Churn") == "No", 0.)
        .otherwise(F.raise_error(F.lit("Invalid Churn label")).cast("double")))
    train = pandas_partitions["train"]
    if train.Churn.value_counts().min() < settings["cv_folds"]:
        raise ValueError("Training class counts are smaller than spark.cv_folds.")
    folds = np.empty(len(train), dtype=int)
    for index, (_, heldout) in enumerate(StratifiedKFold(
        n_splits=settings["cv_folds"], shuffle=True, random_state=settings["seed"]
    ).split(train, train.Churn)):
        folds[heldout] = index
    fold_path = Path(config_path).resolve().parent / ".spark-runtime" / f"folds-{batch_id}.csv"
    train[["customerID"]].assign(cv_fold=folds).to_csv(fold_path, index=False)
    fold_frame = spark.read.option("header", True).csv(fold_path.as_uri())
    partitions = {}
    for name, expected in pandas_partitions.items():
        frame = joined.filter(F.col("split") == name).drop("split")
        if name == "train":
            frame = frame.join(fold_frame, "customerID", "inner")
        frame = frame.cache()
        actual_ids = [row.customerID for row in frame.select("customerID").orderBy("customerID").collect()]
        if actual_ids != expected.customerID.tolist():
            raise ValueError(f"Spark {name} customers differ from authoritative saved memberships.")
        partitions[name] = frame
    return partitions


def run_spark_training(config_path: Path, *, track=True):
    config_path = Path(config_path).resolve()
    root = config_path.parent
    settings = load_spark_settings(config_path)
    total_started = time.perf_counter()
    pandas_partitions, manifest = load_training_partitions(config_path)
    batch_id = uuid.uuid4().hex
    artifacts = root / settings["artifact_dir"] / "runs" / batch_id
    artifacts.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    spark = create_spark(settings, root)
    startup_seconds = time.perf_counter() - started
    partitions = {}
    try:
        started = time.perf_counter()
        partitions = load_spark_partitions(spark, config_path, pandas_partitions, batch_id, settings)
        data_seconds = time.perf_counter() - started
        results, models = {}, {}
        for name, specification in settings["models"].items():
            cv_started = time.perf_counter()
            searches = []
            for overrides in ParameterGrid(specification.get("grid", {})):
                parameters = {**specification["parameters"], **overrides}
                scores = []
                for fold in range(settings["cv_folds"]):
                    logger.info("Spark %s %s fold %s/%s", name, overrides, fold + 1, settings["cv_folds"])
                    training = partitions["train"].filter(F.col("cv_fold").cast("int") != fold)
                    heldout = partitions["train"].filter(F.col("cv_fold").cast("int") == fold)
                    model, _ = fit_candidate(name, parameters, training, settings["seed"])
                    predicted = collect_scores(model, heldout)
                    scores.append(evaluate_probabilities(predicted.label, predicted.churn_probability)["average_precision"])
                searches.append({"parameters": parameters, "fold_scores": scores,
                                 "mean_average_precision": float(np.mean(scores)), "std_average_precision": float(np.std(scores))})
            best = max(searches, key=lambda search: search["mean_average_precision"])
            cv_seconds = time.perf_counter() - cv_started
            logger.info("Spark %s final training fit", name)
            model, timing = fit_candidate(name, best["parameters"], partitions["train"], settings["seed"])
            started = time.perf_counter()
            validation = collect_scores(model, partitions["validation"])
            validation_seconds = time.perf_counter() - started
            threshold = select_threshold(validation.label, validation.churn_probability)
            identity = {"engine": "pyspark", "model_name": name, "parameters": best["parameters"],
                        "dataset_sha256": manifest["source_sha256"], "split_sha256": manifest["membership_sha256"],
                        "seed": settings["seed"], "decision_threshold": threshold["threshold"],
                        "cv_folds": settings["cv_folds"], "shuffle_partitions": settings["shuffle_partitions"],
                        "preprocessing_sha256": sha256_file(Path(__file__).with_name("spark_pipeline.py")),
                        "spark_version": spark.version, "xgboost_version": xgboost.__version__}
            version = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16]
            model.stages.append(SparkChurnDecision(threshold["threshold"], version))
            directory = artifacts / name
            started = time.perf_counter()
            model.save(directory.as_uri())
            save_seconds = time.perf_counter() - started
            started = time.perf_counter()
            loaded = PipelineModel.load(directory.as_uri())
            load_seconds = time.perf_counter() - started
            after = collect_scores(loaded, partitions["validation"].drop("Churn", "label"))
            np.testing.assert_allclose(after.churn_probability, validation.churn_probability, rtol=0, atol=1e-10)
            np.testing.assert_array_equal(after.prediction, np.where(validation.churn_probability >= threshold["threshold"], "Yes", "No"))
            validation.to_csv(artifacts / f"{name}_validation_scores.csv", index=False)
            results[name] = {
                "parameters": best["parameters"], "cv_results": searches, "cv_seconds": cv_seconds,
                "timing": {**timing, "validation_inference_seconds": validation_seconds,
                           "save_seconds": save_seconds, "load_seconds": load_seconds},
                "decision_threshold": threshold["threshold"], "threshold_search": threshold,
                "validation": evaluate_probabilities(validation.label, validation.churn_probability, threshold["threshold"]),
                "validation_default": evaluate_probabilities(validation.label, validation.churn_probability),
                "model_version": version, "pipeline_path": directory.relative_to(root).as_posix(),
                "reloaded_probabilities_match": True, "reloaded_predictions_match": True,
                "preprocessing": {
                    "numeric_medians": model.stages[1].surrogateDF.first().asDict(),
                    "category_labels": model.stages[2].labelsArray,
                    "numeric_features": list(NUMERIC_COLUMNS), "feature_columns": list(FEATURE_COLUMNS),
                    "unknown_category_policy": "StringIndexer keep; OHE drops unknown bucket to all-zero vector",
                    "numeric_scaling": "StandardScaler, sample standard deviation" if name == "logistic_regression" else "none",
                },
            }
            models[name] = loaded
            logger.info("Spark %s validation AP %.4f; threshold %.6f", name, results[name]["validation"]["average_precision"], threshold["threshold"])
        selected = max(settings["models"], key=lambda name: results[name]["validation"]["average_precision"])
        # Freeze this choice before any held-out test scores are collected.
        test_runs, predictions = [], None
        for _ in range(settings["inference_repeats"] + 1):
            started = time.perf_counter()
            predictions = collect_scores(models[selected], partitions["test"].drop("Churn", "label"))
            test_runs.append(time.perf_counter() - started)
        expected_test = pandas_partitions["test"]
        if predictions.customerID.tolist() != expected_test.customerID.tolist():
            raise ValueError("Test customer alignment failed.")
        labels = expected_test.Churn.map({"No": 0, "Yes": 1}).astype(int)
        selected_test_metrics = evaluate_probabilities(labels, predictions.churn_probability, results[selected]["decision_threshold"])
        predictions.to_csv(artifacts / "test_predictions.csv", index=False)
        versions = {p: importlib.metadata.version(p) for p in ("pyspark", "pandas", "numpy", "scikit-learn", "pyarrow")}
        versions["xgboost"] = xgboost.__version__
        report = {
            "status": "PASS", "engine": "pyspark", "batch_id": batch_id,
            "selected_model": selected, "selected_model_version": results[selected]["model_version"],
            "selected_pipeline_path": results[selected]["pipeline_path"],
            "decision_threshold": results[selected]["decision_threshold"],
            "selection_metric": "average_precision", "threshold_metric": "validation_f1",
            "test_used_for_selection": False, "partition_rows": {k: len(v) for k, v in pandas_partitions.items()},
            "candidates": results, "selected_test_metrics": selected_test_metrics,
            "provenance": {"source_sha256": manifest["source_sha256"], "membership_sha256": manifest["membership_sha256"],
                           "test_customer_sha256": hashlib.sha256("\n".join(expected_test.customerID).encode()).hexdigest(),
                           "seed": settings["seed"], "cv_folds": settings["cv_folds"], "runtime": versions,
                           "python": platform.python_version(), "platform": platform.platform(),
                           "spark_master": spark.sparkContext.master, "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")},
            "timing": {"spark_startup_seconds": startup_seconds, "data_load_cache_seconds": data_seconds,
                       "first_test_inference_seconds": test_runs[0], "warm_test_inference_seconds": test_runs[1:],
                       "warm_test_inference_median_seconds": float(np.median(test_runs[1:])),
                       "workflow_seconds_before_comparison_tracking": time.perf_counter() - total_started},
            "timing_note": "Local cached raw frames. Inference executes transform and collects actual probabilities and predictions. Includes JVM execution and collection, excludes metric calculation. First test action is retained separately; no multi-node speedup claim.",
            "metric_note": "Same sklearn metric functions for both engines: churn=Yes precision/recall/F1 and non-interpolated average precision. No Spark weighted-F1 or interpolated areaUnderPR substitution.",
        }
        write_json(artifacts / "metadata.json", report)
        write_json(root / settings["artifact_dir"] / "selected_model.json", {
            "pipeline_path": report["selected_pipeline_path"], "model_version": report["selected_model_version"],
            "metadata_path": (artifacts / "metadata.json").relative_to(root).as_posix(), "batch_id": batch_id})
        write_json(root / settings["report_path"], report)
        from .engine_comparison import compare_engines
        comparison = compare_engines(config_path, report, pandas_partitions)
        if track:
            from .spark_tracking import log_spark_runs
            log_spark_runs(config_path)
        logger.info("Selected Spark %s; test F1 %.4f; AP %.4f", selected, selected_test_metrics["f1"], selected_test_metrics["average_precision"])
        return report
    finally:
        for frame in partitions.values():
            frame.unpersist()
        spark.stop()
