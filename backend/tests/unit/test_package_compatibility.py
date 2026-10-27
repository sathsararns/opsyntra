"""Reorganization must preserve artifacts serialized with historical class paths."""
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import joblib
import numpy as np
from sklearn.dummy import DummyClassifier
from sklearn.pipeline import Pipeline

from opsyntra.ml.inference import load_pipeline, predict_records
from opsyntra.ml.preprocessing import NormalizeTelcoInputs, build_preprocessor
from tests.helpers import sample_data

SOURCE = Path(__file__).resolve().parents[2] / "src"


class PackageCompatibilityTests(unittest.TestCase):
    def test_old_and_new_paths_share_the_same_classes_and_module_state(self):
        for old, new in (
            ("preprocessing_sklearn", "ml.preprocessing"),
            ("kafka_recovery", "kafka.recovery"),
            ("orchestration_models", "orchestration.models"),
        ):
            with self.subTest(module=old):
                self.assertIs(importlib.import_module("opsyntra." + old),
                              importlib.import_module("opsyntra." + new))

    def test_legacy_joblib_loads_in_a_fresh_process_with_identical_predictions(self):
        frame = sample_data(20)
        pipeline = Pipeline([
            ("normalize", NormalizeTelcoInputs()),
            ("preprocessing", build_preprocessor(scale_numeric=False)),
            ("classifier", DummyClassifier(strategy="prior")),
        ]).fit(frame, frame.Churn.map({"No": 0, "Yes": 1}))
        pipeline.opsyntra_metadata_ = {"model_version": "legacy-test"}
        expected = predict_records(pipeline, frame.drop(columns="Churn"))
        importlib.import_module("opsyntra.preprocessing_sklearn")
        with tempfile.TemporaryDirectory() as folder:
            model = Path(folder) / "legacy.joblib"
            record = Path(folder) / "customers.json"
            record.write_text(frame.drop(columns="Churn").to_json(orient="records"), encoding="utf-8")
            with patch.object(NormalizeTelcoInputs, "__module__", "opsyntra.preprocessing_sklearn"):
                joblib.dump(pipeline, model)
            script = (
                "import json,sys; from opsyntra.ml.inference import load_pipeline,predict_records; "
                "result=predict_records(load_pipeline(sys.argv[1]),json.load(open(sys.argv[2]))); "
                "print(result.to_json(orient='records',double_precision=15))"
            )
            output = subprocess.check_output([sys.executable, "-c", script, str(model), str(record)],
                text=True, env={**os.environ, "PYTHONPATH": str(SOURCE)})
            actual = json.loads(output)
            np.testing.assert_allclose([r["churn_probability"] for r in actual], expected.churn_probability,
                                       rtol=0, atol=1e-12)
            self.assertEqual([r["prediction"] for r in actual], expected.prediction.tolist())
            self.assertTrue(all(r["model_version"] == "legacy-test" for r in actual))
            self.assertIsInstance(load_pipeline(model).named_steps["normalize"], NormalizeTelcoInputs)

    def test_plain_package_import_does_not_load_optional_services(self):
        script = (
            "import sys,opsyntra; "
            "assert not {'pyspark','mlflow','confluent_kafka','psycopg','pandas'} & set(sys.modules)"
        )
        subprocess.run([sys.executable, "-c", script], check=True,
                       env={**os.environ, "PYTHONPATH": str(SOURCE)})
