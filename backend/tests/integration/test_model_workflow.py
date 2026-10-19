import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd
import yaml
import mlflow
import mlflow.sklearn

from opsyntra.data_pipeline import run_pipeline
from opsyntra.model_inference import load_pipeline, predict_records
from opsyntra.model_training import load_training_partitions, run_training
from tests.helpers import sample_data

ROOT = Path(__file__).resolve().parents[2]


class ModelWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(cls.temporary.name)
        settings = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
        settings["training"]["cv_folds"] = 2
        settings["training"]["estimator_jobs"] = 1
        settings["training"]["param_grids"] = {
            "dummy": {}, "logistic_regression": {"classifier__C": [1.]},
            "random_forest": {"classifier__n_estimators": [5]},
            "xgboost": {"classifier__n_estimators": [5]},
        }
        cls.config_path = cls.root / "config.yaml"
        cls.config_path.write_text(yaml.safe_dump(settings), encoding="utf-8")
        cls.tracking_uri = "sqlite:///" + (cls.root / settings["training"]["tracking"]["database"]).as_posix()
        # Release pooled SQLite handles before TemporaryDirectory cleanup on Windows.
        cls.addClassCleanup(lambda: mlflow.MlflowClient(cls.tracking_uri)._tracking_client.store.engine.dispose())
        archive_path = cls.root / "input.zip"
        with ZipFile(archive_path, "w") as archive:
            archive.writestr(settings["data"]["archive_member"], sample_data().to_csv(index=False))
        run_pipeline(cls.config_path, archive_path)
        cls.report = run_training(cls.config_path)
        cls.model_path = cls.root / "artifacts/models/best_pipeline.joblib"
        cls.pipeline = load_pipeline(cls.model_path)

    def test_artifact_contains_pipeline_and_selection_did_not_use_test(self):
        self.assertFalse(self.report["test_used_for_selection"])
        self.assertEqual(set(self.report["candidates"]), {"dummy", "logistic_regression", "random_forest", "xgboost"})
        self.assertEqual(self.report["partition_rows"], {"train": 60, "validation": 20, "test": 20})
        self.assertTrue(self.report["artifact"]["reload_probabilities_match"])
        self.assertTrue(self.report["artifact"]["reload_classifications_identical"])
        for candidate in self.report["candidates"].values():
            self.assertNotIn("test", candidate)
        self.assertEqual(self.pipeline.opsyntra_metadata_["fit_partition"], "train")

    def test_fresh_process_single_and_batch_inference_match_in_memory(self):
        rows = sample_data().drop(columns="Churn").iloc[:3]
        record_path = self.root / "record.json"
        csv_path = self.root / "records.csv"
        json_output = self.root / "single_prediction.json"
        csv_output = self.root / "batch_predictions.csv"
        record_path.write_text(json.dumps(rows.iloc[0].to_dict()), encoding="utf-8")
        rows.to_csv(csv_path, index=False)
        for arguments in (
            ["--record", str(record_path), "--output", str(json_output)],
            ["--input", str(csv_path), "--output", str(csv_output)],
        ):
            process = subprocess.run(
                [sys.executable, "-m", "pipelines.inference_pipeline", "--config", str(self.config_path), *arguments],
                cwd=ROOT, text=True, capture_output=True, timeout=60,
            )
            self.assertEqual(process.returncode, 0, process.stderr)
        expected = predict_records(self.pipeline, rows)
        batch = pd.read_csv(csv_output)
        single = json.loads(json_output.read_text())
        np.testing.assert_allclose(batch["churn_probability"], expected["churn_probability"], rtol=0, atol=1e-14)
        self.assertAlmostEqual(single["churn_probability"], expected.iloc[0]["churn_probability"], places=14)
        self.assertEqual(batch["prediction"].tolist(), expected["prediction"].tolist())
        self.assertEqual(batch["customerID"].tolist(), rows["customerID"].tolist())

    def test_inference_output_cannot_overwrite_raw_input(self):
        raw_path = self.root / "data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv"
        original = raw_path.read_bytes()
        process = subprocess.run(
            [sys.executable, "-m", "pipelines.inference_pipeline", "--config", str(self.config_path),
             "--input", str(ROOT / "examples/customers.csv"), "--output", str(raw_path)],
            cwd=ROOT, text=True, capture_output=True, timeout=60,
        )
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("must differ", process.stderr)
        self.assertEqual(raw_path.read_bytes(), original)

    def test_changed_membership_is_rejected_before_training(self):
        path = self.root / "data/splits/memberships.csv"
        original = path.read_bytes()
        try:
            path.write_bytes(original + b"\n")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                load_training_partitions(self.config_path)
        finally:
            path.write_bytes(original)

    def test_tracking_has_comparable_runs_artifacts_and_test_only_for_winner(self):
        client = mlflow.MlflowClient(self.tracking_uri)
        self.assertEqual(len(self.report["tracking"]["runs"]), 4)
        for name, run_id in self.report["tracking"]["runs"].items():
            run = client.get_run(run_id)
            self.assertEqual(run.info.status, "FINISHED")
            self.assertEqual(run.data.params["dataset_sha256"], self.report["provenance"]["source_sha256"])
            self.assertEqual(run.data.params["split_sha256"], self.report["provenance"]["membership_sha256"])
            for metric in ("precision", "recall", "f1", "roc_auc", "average_precision", "accuracy"):
                self.assertIn("validation_" + metric, run.data.metrics)
            self.assertEqual("test_f1" in run.data.metrics, name == self.report["selected_model"])
            self.assertTrue(client.list_artifacts(run_id, "joblib"))
            self.assertTrue(client.list_artifacts(run_id, "plots"))
            self.assertTrue(client.list_artifacts(run_id, "preprocessing"))
        self.assertTrue((self.root / "reports/model_selection.md").is_file())

    def test_mlflow_model_reloads_with_the_persisted_threshold(self):
        old_uri = mlflow.get_tracking_uri()
        try:
            mlflow.set_tracking_uri(self.tracking_uri)
            result = self.report["candidates"][self.report["selected_model"]]
            loaded = mlflow.sklearn.load_model(result["mlflow_model_uri"])
            rows = sample_data().drop(columns="Churn")
            np.testing.assert_array_equal(loaded.predict(rows), self.pipeline.predict(rows))
            if self.report["selected_model"] != "dummy":
                np.testing.assert_array_equal(self.pipeline.predict(rows),
                    (self.pipeline.predict_proba(rows)[:, 1] >= self.report["decision_threshold"]).astype(int))
        finally:
            mlflow.set_tracking_uri(old_uri)
