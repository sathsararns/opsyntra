"""Container startup must preserve trusted data/models and avoid sharing credentials."""
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("container_runtime_tests", ROOT / "containers/runtime.py")
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


class ContainerRuntimeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "config.yaml").write_text((ROOT / "backend/config.yaml").read_text())
        self.config_path = self.root / "generated.yaml"
        self.environment = patch.dict(os.environ, {"OPSYNTRA_CONFIG": str(self.config_path)}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.root_patch = patch.object(runtime, "ROOT", self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)

    def test_runtime_config_resolves_shared_paths_without_host_venv(self):
        path, config = runtime.prepare_config()
        self.assertEqual(path, self.config_path)
        self.assertEqual(Path(config["splitting"]["output_dir"]), self.root / "data/splits")
        self.assertEqual(Path(config["training"]["tracking"]["database"]), self.root / "tracking/mlflow.db")
        with patch.dict(os.environ, {"OPSYNTRA_ARTIFACT_DIR": str(self.root / "alternate")}):
            _, config = runtime.prepare_config()
        self.assertEqual(Path(config["kafka"]["consumer"]["state_dir"]), self.root / "alternate/kafka/consumer_state")
        self.assertNotIn(".venv-spark", path.read_text())

    def test_matching_model_is_reused_but_tampering_is_rejected(self):
        path, config = runtime.prepare_config()
        source = self.root / "input.csv"
        source.write_text("trusted csv\n")
        raw = Path(config["data"]["raw_path"])
        raw.parent.mkdir(parents=True)
        raw.write_bytes(source.read_bytes())
        model = Path(config["training"]["artifact_dir"]) / "best_pipeline.joblib"
        model.parent.mkdir(parents=True)
        model.write_bytes(b"trusted model")
        receipt = {"source_sha256": runtime.digest(source), "config_sha256": runtime.digest(path), "model_sha256": runtime.digest(model)}
        (model.parent / "container-training.json").write_text(json.dumps(receipt))
        with patch("opsyntra.data.pipeline.run_pipeline") as validate, patch("opsyntra.ml.training.run_training") as train:
            runtime.model_init(path, config, source=source)
            validate.assert_not_called()
            train.assert_not_called()
            model.write_bytes(b"changed model")
            with self.assertRaisesRegex(ValueError, "receipt"):
                runtime.model_init(path, config, source=source)
            train.assert_not_called()

    def test_different_source_never_overwrites_persistent_data(self):
        path, config = runtime.prepare_config()
        source = self.root / "input.csv"
        source.write_text("different\n")
        raw = Path(config["data"]["raw_path"])
        raw.parent.mkdir(parents=True)
        raw.write_text("original\n")
        with self.assertRaisesRegex(ValueError, "Dataset differs"):
            runtime.model_init(path, config, source=source)
        self.assertEqual(raw.read_text(), "original\n")

    def test_role_url_escapes_password_and_rejects_placeholders(self):
        with patch.dict(os.environ, {"ANALYTICS_READ_PASSWORD": "a@b:/c"}):
            self.assertEqual(runtime.role_url("read"), "postgresql://opsyntra_read:a%40b%3A%2Fc@postgres:5432/opsyntra")
        with patch.dict(os.environ, {"ANALYTICS_READ_PASSWORD": "CHANGE_ME_READ"}):
            with self.assertRaisesRegex(ValueError, "placeholders"):
                runtime.role_url("read")
