"""Functional task-contract checks without importing or starting Airflow."""
import json
import hashlib
import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from opsyntra.airflow_workflow import WorkflowContext, TASKS, execute_task
from opsyntra.data_pipeline import run_pipeline
from tests.helpers import sample_data

ROOT = Path(__file__).resolve().parents[2]


class AirflowWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        settings = yaml.safe_load((ROOT / "config.yaml").read_text())
        settings["training"]["cv_folds"] = 2
        settings["training"]["estimator_jobs"] = 1
        settings["training"]["param_grids"] = {
            "dummy": {}, "logistic_regression": {"classifier__C": [1.]},
            "random_forest": {"classifier__n_estimators": [5]}, "xgboost": {"classifier__n_estimators": [5]},
        }
        (self.root / "config.yaml").write_text(yaml.safe_dump(settings))
        raw = self.root / settings["data"]["raw_path"]
        raw.parent.mkdir(parents=True)
        sample_data().to_csv(raw, index=False)
        run_pipeline(self.root / "config.yaml")
        (self.root / "examples").mkdir()
        sample_data().iloc[:3].drop(columns="Churn").to_csv(self.root / "examples/customers.csv", index=False)
        self.ctx = WorkflowContext(self.root, "manual__fixture", "sklearn")

    def run_until(self, task):
        for name in TASKS:
            execute_task(name, self.ctx)
            if name == task:
                break

    def test_complete_sequence_reuses_memberships_and_retry_does_not_duplicate_results(self):
        self.run_until("save_results")
        ctx = self.ctx
        self.assertEqual((ctx.run / "source/memberships.csv").read_bytes(),
                         (ctx.run / "data/splits/memberships.csv").read_bytes())
        report = ctx.read("results/summary.json")
        self.assertFalse(report["test_used_for_selection"])
        self.assertEqual(report["prediction_rows"], 3)
        self.assertTrue(all(ctx.read(f"logs/{name}_attempt_1.json")["status"] == "success" for name in TASKS))
        self.assertEqual(ctx.read("approval.json")["status"], "PASS")
        original = (ctx.run / "results/predictions.csv").read_bytes()
        execute_task("predict", ctx, attempt=2)
        execute_task("save_results", ctx, attempt=2)
        self.assertEqual((ctx.run / "results/predictions.csv").read_bytes(), original)
        self.assertEqual(len(pd.read_csv(ctx.run / "results/predictions.csv")), 3)
        self.assertTrue((ctx.artifact_root / "latest_success_sklearn.json").is_file())
        self.assertFalse((self.root / "artifacts/models/best_pipeline.joblib").exists())

    def test_failed_gate_blocks_selected_model_and_success_pointer(self):
        self.run_until("evaluate")
        evaluation = self.ctx.read("evaluation.json")
        evaluation["candidates"][evaluation["selected_model"]]["validation"]["average_precision"] = 0.
        self.ctx.write("evaluation.json", evaluation)
        with self.assertRaisesRegex(ValueError, "Candidate rejected"):
            execute_task("validate_candidate", self.ctx)
        with self.assertRaisesRegex(ValueError, "approval"):
            execute_task("save_selected_model", self.ctx)
        self.assertFalse((self.ctx.run / "selected_model.json").exists())
        self.assertFalse((self.ctx.artifact_root / "latest_success_sklearn.json").exists())

    def test_changed_snapshot_and_changed_approved_model_fail(self):
        self.run_until("validate_candidate")
        evaluation = self.ctx.read("evaluation.json")
        candidate = evaluation["candidates"][evaluation["selected_model"]]
        with self.ctx.resolve(candidate["path"]).open("ab") as output:
            output.write(b"tampered")
        with self.assertRaisesRegex(ValueError, "approval"):
            execute_task("save_selected_model", self.ctx)
        with (self.ctx.run / "source/raw.csv").open("ab") as output:
            output.write(b"\n")
        with self.assertRaisesRegex(ValueError, "snapshot changed"):
            execute_task("prepare_splits", self.ctx)

    def test_run_paths_are_distinct_and_snapshot_is_reused(self):
        execute_task("validate_data", self.ctx)
        snapshot = self.ctx.read("snapshot.json")
        result = execute_task("validate_data", self.ctx, attempt=2)
        self.assertTrue(result["snapshot_reused"])
        self.assertEqual(self.ctx.read("snapshot.json"), snapshot)
        other = WorkflowContext(self.root, "../manual__fixture", "sklearn")
        self.assertNotEqual(other.run, self.ctx.run)
        self.assertTrue(other.run.is_relative_to(self.ctx.artifact_root))
        with self.assertRaisesRegex(ValueError, "escapes"):
            self.ctx.resolve("../../../../outside")


class NativeArtifactPathTests(unittest.TestCase):
    def test_deep_spark_files_are_included_in_integrity_checks(self):
        # Regression: Windows Path.is_file silently omitted files beyond 260
        # characters, producing an apparently valid but incomplete model archive.
        from opsyntra.orchestration_models import filesystem_path, model_digest
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, filesystem_path(root))
        relative = Path("stages") / ("SparkStage_" + "x" * 100) / "data" / ("part-" + "y" * 100 + ".parquet")
        target = filesystem_path(root / relative)
        target.parent.mkdir(parents=True)
        target.write_bytes(b"native model weights")
        expected = hashlib.sha256(relative.as_posix().encode() + b"native model weights").hexdigest()
        self.assertEqual(model_digest(root), expected)
        target.write_bytes(b"changed model weights")
        self.assertNotEqual(model_digest(root), expected)
