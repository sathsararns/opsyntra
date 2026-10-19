import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

import yaml

from opsyntra.config import load_config
from opsyntra.data_pipeline import run_pipeline
from opsyntra.data_validation import DataValidationError
from tests.helpers import sample_data


class DataPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        settings = yaml.safe_load((Path(__file__).parents[2] / "config.yaml").read_text(encoding="utf-8"))
        self.config_path = self.root / "config.yaml"
        self.config_path.write_text(yaml.safe_dump(settings), encoding="utf-8")
        self.config = load_config(self.config_path)
        self.archive = self.root / "input.zip"
        with ZipFile(self.archive, "w") as archive:
            archive.writestr(self.config.archive_member, sample_data().to_csv(index=False))

    def test_archive_import_outputs_and_byte_for_byte_rerun(self):
        report = run_pipeline(self.config_path, self.archive)
        self.assertEqual(report["status"], "PASS_WITH_WARNINGS")
        self.assertEqual(report["rows"], 100)
        paths = [self.config.raw_path, self.config.normalized_path, self.config.report_path] + list(self.config.output_dir.iterdir())
        before = {path: path.read_bytes() for path in paths}
        run_pipeline(self.config_path, self.archive)
        self.assertEqual(before, {path: path.read_bytes() for path in paths})
        manifest = json.loads((self.config.output_dir / "manifest.json").read_text())
        self.assertTrue(all(manifest["checks"].values()))

    def test_existing_raw_data_is_never_overwritten_by_a_different_archive(self):
        run_pipeline(self.config_path, self.archive)
        original = self.config.raw_path.read_bytes()
        with ZipFile(self.archive, "w") as archive:
            archive.writestr(self.config.archive_member, "different content")
        with self.assertRaises(ValueError):
            run_pipeline(self.config_path, self.archive)
        self.assertEqual(self.config.raw_path.read_bytes(), original)

    def test_invalid_data_writes_a_failure_report_without_creating_splits(self):
        self.config.raw_path.parent.mkdir(parents=True)
        data = sample_data()
        data.loc[0, "TotalCharges"] = "corrupted"
        data.to_csv(self.config.raw_path, index=False)
        with self.assertRaises(DataValidationError):
            run_pipeline(self.config_path)
        self.assertEqual(json.loads(self.config.report_path.read_text())["status"], "FAIL")
        self.assertFalse(self.config.output_dir.exists())

    def test_infinity_produces_a_serializable_failure_report(self):
        self.config.raw_path.parent.mkdir(parents=True)
        data = sample_data()
        data.loc[0, "MonthlyCharges"] = "inf"
        data.to_csv(self.config.raw_path, index=False)
        with self.assertRaises(DataValidationError):
            run_pipeline(self.config_path)
        report = json.loads(self.config.report_path.read_text())
        self.assertEqual(report["status"], "FAIL")
        self.assertTrue(any("infinite" in error for error in report["errors"]))

    def test_invalid_fractions_and_input_output_collisions_fail(self):
        settings = yaml.safe_load(self.config_path.read_text())
        settings["splitting"]["train"] = .8
        self.config_path.write_text(yaml.safe_dump(settings))
        with self.assertRaises(ValueError):
            load_config(self.config_path)
        settings["splitting"]["train"] = .6
        settings["data"]["normalized_path"] = settings["data"]["raw_path"]
        self.config_path.write_text(yaml.safe_dump(settings))
        with self.assertRaises(ValueError):
            load_config(self.config_path)
