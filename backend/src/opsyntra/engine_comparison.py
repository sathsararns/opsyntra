"""Compare fixed engine champions and explicitly separated local timings."""

import json
import platform
import time
from pathlib import Path

import numpy as np
import pandas as pd

from .config import load_training_config
from .data_pipeline import sha256_file, write_json
from .data_validation import FEATURE_COLUMNS
from .model_building import build_model_pipeline
from .model_evaluation import evaluate_probabilities
from .model_inference import load_pipeline


def compare_engines(config_path, spark_report, partitions):
    from .spark_training import load_spark_settings
    root = Path(config_path).resolve().parent
    settings = load_spark_settings(config_path)
    config = load_training_config(config_path)
    sklearn_report = json.loads(config.report_path.read_text(encoding="utf-8"))
    for key in ("source_sha256", "membership_sha256"):
        if sklearn_report["provenance"][key] != spark_report["provenance"][key]:
            raise ValueError("Cannot compare engines trained on different datasets or memberships.")
    model_path = config.artifact_dir / "best_pipeline.joblib"
    if sha256_file(model_path) != sklearn_report["artifact"]["sha256"]:
        raise ValueError("Saved sklearn artifact does not match its report.")
    started = time.perf_counter()
    saved = load_pipeline(model_path)
    load_seconds = time.perf_counter() - started
    if saved.opsyntra_metadata_["model_version"] != sklearn_report["selected_model_version"]:
        raise ValueError("Sklearn artifact version does not match its report.")
    name = sklearn_report["selected_model"]
    # A fresh single final fit is comparable to Spark final fit, not its CV search.
    refit = build_model_pipeline(name, seed=config.seed, jobs=config.estimator_jobs)
    refit.set_params(**sklearn_report["candidates"][name]["best_parameters"])
    train, validation, test = (partitions[k] for k in ("train", "validation", "test"))
    features = list(FEATURE_COLUMNS)
    y_train = train.Churn.map({"No": 0, "Yes": 1}).astype(int)
    started = time.perf_counter()
    transformed = refit[:-1].fit_transform(train[features], y_train)
    preprocessing_seconds = time.perf_counter() - started
    started = time.perf_counter()
    refit.named_steps["classifier"].fit(transformed, y_train)
    classifier_fit_seconds = time.perf_counter() - started
    np.testing.assert_allclose(refit.predict_proba(validation[features]), saved.predict_proba(validation[features]), rtol=0, atol=1e-9)
    raw_test = test[features]
    threshold = sklearn_report.get("decision_threshold", .5)
    inference, predictions = [], None
    for _ in range(settings["inference_repeats"] + 1):
        started = time.perf_counter()
        probabilities = saved.predict_proba(raw_test)[:, 1]
        predictions = pd.DataFrame({"customerID": test.customerID, "churn_probability": probabilities,
                                    "prediction": np.where(probabilities >= threshold, "Yes", "No")}).sort_values("customerID").reset_index(drop=True)
        inference.append(time.perf_counter() - started)
    spark_predictions_path = root / Path(spark_report["selected_pipeline_path"]).parent / "test_predictions.csv"
    spark_predictions = pd.read_csv(spark_predictions_path, dtype={"customerID": "string"})
    if predictions.customerID.tolist() != spark_predictions.customerID.tolist():
        raise ValueError("Engine comparison requires exactly the same test customer IDs and order.")
    metrics = evaluate_probabilities(test.Churn.map({"No": 0, "Yes": 1}), predictions.churn_probability, threshold)
    for metric, value in metrics.items():
        np.testing.assert_allclose(value, sklearn_report["selected_test_metrics"][metric], rtol=0, atol=1e-10)
    benchmark = {"preprocessing_seconds": preprocessing_seconds, "classifier_fit_seconds": classifier_fit_seconds,
                 "fit_total_seconds": preprocessing_seconds + classifier_fit_seconds, "load_seconds": load_seconds,
                 "first_test_inference_seconds": inference[0], "warm_test_inference_seconds": inference[1:],
                 "warm_test_inference_median_seconds": float(np.median(inference[1:]))}
    spark_best = spark_report["candidates"][spark_report["selected_model"]]
    paired = test[["customerID", "Churn"]].copy()
    paired["sklearn_probability"] = predictions.churn_probability.to_numpy()
    paired["sklearn_prediction"] = predictions.prediction.to_numpy()
    paired["spark_probability"] = spark_predictions.churn_probability.to_numpy()
    paired["spark_prediction"] = spark_predictions.prediction.to_numpy()
    output = root / settings["comparison_path"]
    paired.to_csv(output.parent / "engine_test_predictions.csv", index=False, lineterminator="\n")
    report = {
        "status": "PASS", "same_test_customers": True, "test_rows": len(test),
        "test_customer_sha256": spark_report["provenance"]["test_customer_sha256"],
        "dataset_sha256": spark_report["provenance"]["source_sha256"],
        "membership_sha256": spark_report["provenance"]["membership_sha256"],
        "spark_batch_id": spark_report["batch_id"],
        "sklearn": {"selected_model": name, "model_version": sklearn_report["selected_model_version"],
                    "decision_threshold": threshold, "test_metrics": metrics, "timing": benchmark,
                    "original_cv_and_fit_seconds": sklearn_report["candidates"][name]["training_seconds"],
                    "original_training_runtime": sklearn_report["provenance"]["runtime"]},
        "spark": {"selected_model": spark_report["selected_model"], "model_version": spark_report["selected_model_version"],
                  "decision_threshold": spark_report["decision_threshold"], "test_metrics": spark_report["selected_test_metrics"],
                  "timing": {**spark_best["timing"], **spark_report["timing"]}},
        "benchmark_platform": platform.platform(),
        "selection_rule": "Each engine selects its champion by validation AP after training-only stratified CV. Thresholds maximize validation churn-class F1. Test never selects an engine or candidate.",
        "fairness_notes": [
            "Identical saved train/validation/test customers, raw predictors, target mapping and sklearn metric definitions.",
            "Spark and sklearn use different solvers, regularization conventions, tree construction and tuning grids; this is a pipeline comparison, not numerical algorithm equivalence.",
            "Spark's exact median quantile uses an order statistic; sklearn averages middle values for even-sized samples. Spark StandardScaler uses sample standard deviation; sklearn uses population standard deviation.",
            "Models fit training rows only, including every CV fold's imputation and category vocabularies. Unknown categorical values map to all-zero one-hot blocks in both engines.",
            "Fresh sklearn final-fit timing excludes grid search. Spark final preprocessing/fit excludes its CV search, separately reported. No CV-versus-single-fit speed ratio.",
            "Inference uses the complete saved pipelines, raw records cached/in memory, actual probability calculation, and collection into ID-sorted pandas tables. First action and subsequent warm timings are separate.",
            "Spark JVM startup and data loading/cache materialization are separate. These are local single-machine measurements; they do not demonstrate distributed scaling.",
            "Report metrics use the existing saved sklearn artifact. The Linux timing refit is checked against its validation probabilities; it does not replace the deployed sklearn model.",
        ],
    }
    write_json(output, report)
    lines = ["# Opsyntra: scikit-learn and PySpark comparison", "",
             f"Both champions predict the **same {len(test):,} held-out customers**. Customer IDs and labels are aligned before metrics are computed.", "",
             report["selection_rule"], "", "| Engine | Selected model | Threshold | Accuracy | Precision | Recall | F1 | ROC-AUC | PR average precision |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for engine in ("sklearn", "spark"):
        r = report[engine]
        m = r["test_metrics"]
        lines.append(f"| {engine} | {r['selected_model']} | {r['decision_threshold']:.6f} | " + " | ".join(f"{m[key]:.4f}" for key in ("accuracy", "precision", "recall", "f1", "roc_auc", "average_precision")) + " |")
    lines += ["", "Confusion matrices: rows = actual No/Yes; columns = predicted No/Yes.", "",
              f"- sklearn: `{metrics['confusion_matrix']}`", f"- Spark: `{spark_report['selected_test_metrics']['confusion_matrix']}`", "",
              "## Measured local time", "", "All values are seconds. Training compares a single final fit, with CV searches reported separately.", "",
              "| Engine | Preprocessing | Classifier fit | Final fit total | Warm inference median |",
              "|---|---:|---:|---:|---:|"]
    for engine in ("sklearn", "spark"):
        t = report[engine]["timing"]
        warm = t["warm_test_inference_median_seconds"]
        lines.append(f"| {engine} | {t['preprocessing_seconds']:.4f} | {t['classifier_fit_seconds']:.4f} | {t['fit_total_seconds']:.4f} | {warm:.4f} |")
    lines += ["", f"Spark startup: {spark_report['timing']['spark_startup_seconds']:.4f}s; data load/cache: {spark_report['timing']['data_load_cache_seconds']:.4f}s.", "",
              "## Validation comparison", "", "| Engine | Candidate | Validation AP | Validation F1 | Threshold |", "|---|---|---:|---:|---:|"]
    validation_rows = []
    for engine, source in (("sklearn", sklearn_report), ("spark", spark_report)):
        for candidate, result in source["candidates"].items():
            m = result["validation"]
            lines.append(f"| {engine} | {candidate} | {m['average_precision']:.4f} | {m['f1']:.4f} | {result.get('decision_threshold', .5):.6f} |")
            validation_rows.append({"engine": engine, "model": candidate, "decision_threshold": result.get("decision_threshold", .5),
                                    **{k: v for k, v in m.items() if k != "confusion_matrix"}})
    pd.DataFrame(validation_rows).to_csv(output.parent / "engine_validation_comparison.csv", index=False)
    lines += ["", "## Interpretation and limitations", "", *[f"- {note}" for note in report["fairness_notes"]], "",
              f"Benchmark environment: `{report['benchmark_platform']}`.",
              f"Dataset SHA256: `{report['dataset_sha256']}`.", f"Membership SHA256: `{report['membership_sha256']}`.",
              f"Test customer SHA256: `{report['test_customer_sha256']}`.",
              f"Spark batch: `{spark_report['batch_id']}`. Spark pipeline: `{spark_report['selected_pipeline_path']}`.", "",
              "Full timing samples and metrics: `engine_comparison.json`. Paired held-out predictions: `engine_test_predictions.csv`.", ""]
    output.with_suffix(".md").write_text("\n".join(lines), encoding="utf-8")
    return report
