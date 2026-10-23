"""Real Spark checks, opt-in because they start a JVM and execute XGBoost workers."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(os.environ.get("OPSYNTRA_RUN_SPARK_TESTS") == "1", "Run via start-spark.ps1 -Task test in the Spark runtime")
class SparkWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from opsyntra.spark_runtime import create_spark
        from opsyntra.spark_training import load_spark_settings
        cls.spark = create_spark(load_spark_settings(ROOT / "config.yaml"), ROOT)
        cls.addClassCleanup(cls.spark.stop)
        cls.temporary = tempfile.TemporaryDirectory(dir=ROOT / ".spark-runtime")
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.directory = Path(cls.temporary.name)
        from tests.helpers import sample_data
        cls.data = sample_data(100)
        cls.input_path = cls.directory / "train.csv"
        cls.data.iloc[:60].to_csv(cls.input_path, index=False)
        from pyspark.sql import functions as F
        cls.train = cls.spark.read.option("header", True).csv(cls.input_path.as_uri()).withColumn(
            "label", F.when(F.col("Churn") == "Yes", 1.).otherwise(0.))

    def test_training_statistics_unknown_categories_and_complete_model_reload(self):
        from pyspark.ml import PipelineModel
        from pyspark.ml.functions import vector_to_array
        from opsyntra.spark_pipeline import SparkChurnDecision, CATEGORICAL_COLUMNS
        from opsyntra.spark_training import fit_candidate
        from opsyntra.spark_inference import load_spark_pipeline, predict_spark_frame
        from opsyntra.data_validation import NUMERIC_COLUMNS
        model, _ = fit_candidate("logistic_regression", {"maxIter": 20, "regParam": .01}, self.train, 42)
        medians = model.stages[1].surrogateDF.first().asDict()
        clean = self.data.iloc[:60].copy()
        for column in NUMERIC_COLUMNS:
            values = pd.to_numeric(clean[column].str.strip(), errors="coerce").dropna().sort_values().to_numpy()
            # Spark's exact quantile is the lower middle order statistic.
            expected = values[max(0, int(np.ceil(.5 * len(values))) - 1)]
            self.assertAlmostEqual(medians[column], expected)
        rows = self.data.iloc[60:63].drop(columns="Churn").copy()
        rows["Contract"] = "UNSEEN-CONTRACT"
        rows["TotalCharges"] = " "
        rows_path = self.directory / "records.csv"
        rows.to_csv(rows_path, index=False)
        frame = self.spark.read.option("header", True).csv(rows_path.as_uri())
        encoded = model.stages[0].transform(frame)
        for stage in model.stages[1:4]:
            encoded = stage.transform(encoded)
        contract_index = CATEGORICAL_COLUMNS.index("Contract")
        values = encoded.select(vector_to_array(f"_cat_{contract_index}").alias("values")).collect()
        self.assertTrue(all(sum(row["values"]) == 0 for row in values))
        model.stages.append(SparkChurnDecision(.25, "test-model"))
        expected = predict_spark_frame(model, frame)
        directory = self.directory / "pipeline"
        model.save(directory.as_uri())
        actual = predict_spark_frame(load_spark_pipeline(directory), frame)
        np.testing.assert_allclose(actual.churn_probability, expected.churn_probability, atol=1e-12)
        self.assertEqual(actual.prediction.tolist(), expected.prediction.tolist())
        self.assertEqual(actual.prediction.tolist(), np.where(actual.churn_probability >= .25, "Yes", "No").tolist())
        self.assertEqual(actual.customerID.tolist(), rows.customerID.tolist())
        self.assertEqual(model.stages[1].surrogateDF.first().asDict(), medians)

    def test_all_three_real_estimators_can_fit_and_persist(self):
        from opsyntra.spark_pipeline import SparkChurnDecision
        from opsyntra.spark_training import fit_candidate
        from opsyntra.spark_inference import load_spark_pipeline, predict_spark_frame
        for name, parameters in (
            ("logistic_regression", {"maxIter": 10, "regParam": .01}),
            ("random_forest", {"numTrees": 5, "maxDepth": 3}),
            ("xgboost", {"n_estimators": 5, "max_depth": 2, "num_workers": 1}),
        ):
            with self.subTest(model=name):
                model, _ = fit_candidate(name, parameters, self.train, 42)
                model.stages.append(SparkChurnDecision(.4, name))
                directory = self.directory / f"pipeline-{name}"
                model.save(directory.as_uri())
                result = predict_spark_frame(load_spark_pipeline(directory), self.train.drop("Churn", "label"))
                self.assertEqual(len(result), 60)
                self.assertTrue(result.churn_probability.between(0, 1).all())

    def test_selected_real_pipeline_predicts_in_fresh_single_and_batch_processes(self):
        from opsyntra.spark_training import fit_candidate
        from opsyntra.spark_pipeline import SparkChurnDecision
        # Fit a tiny training-only fixture; clean checkouts have no production model.
        fitted, _ = fit_candidate("logistic_regression", {"maxIter": 10, "regParam": .01}, self.train, 42)
        fitted.stages.append(SparkChurnDecision(.4, "fresh-process-fixture"))
        model_path = self.directory / "selected-fixture"
        fitted.save(model_path.as_uri())
        from opsyntra.spark_inference import load_spark_pipeline, predict_spark_frame
        model = load_spark_pipeline(model_path)
        sample = pd.read_csv(ROOT / "examples/customers.csv", dtype="string", keep_default_na=False)
        frame = self.spark.read.option("header", True).csv((ROOT / "examples/customers.csv").as_uri())
        expected = predict_spark_frame(model, frame)
        for arguments, output in (
            (["--record", str(ROOT / "examples/customer.json")], self.directory / "single.json"),
            (["--input", str(ROOT / "examples/customers.csv")], self.directory / "batch.csv"),
        ):
            process = subprocess.run([sys.executable, "-m", "pipelines.spark_inference_pipeline", "--model", str(model_path), *arguments,
                                      "--output", str(output)], cwd=ROOT, capture_output=True, text=True, timeout=180)
            self.assertEqual(process.returncode, 0, process.stderr[-8000:])
            if output.suffix == ".csv":
                actual = pd.read_csv(output)
                np.testing.assert_allclose(actual.churn_probability, expected.churn_probability, atol=1e-10)
                self.assertEqual(actual.prediction.tolist(), expected.prediction.tolist())
            else:
                result = json.loads(output.read_text())
                reference = expected.loc[expected.customerID == result["customerID"]].iloc[0]
                self.assertAlmostEqual(result["churn_probability"], reference.churn_probability, places=10)

    def test_bad_numeric_and_missing_feature_columns_fail(self):
        from opsyntra.spark_pipeline import NormalizeSparkTelco
        with self.assertRaisesRegex(ValueError, "Missing required predictor"):
            NormalizeSparkTelco().transform(self.train.drop("tenure"))
        from pyspark.sql import functions as F
        with self.assertRaises(Exception):
            NormalizeSparkTelco().transform(self.train.withColumn("TotalCharges", F.lit("corrupt"))).select("TotalCharges").collect()
