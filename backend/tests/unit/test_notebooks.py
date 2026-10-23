"""Guard notebook provenance and prohibit accidental held-out exploration."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from opsyntra.notebook_support import Exports, analytics_snapshot, digest, portable_path, verified_partition


class NotebookInputTests(unittest.TestCase):
    def test_only_requested_partition_is_read_and_tampering_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "raw.csv").write_text("source data\n")
            (root / "memberships.csv").write_text("customerID,split\nA,train\nB,validation\nC,test\n")
            (root / "train.csv").write_text("customerID,TotalCharges,Churn\nA,,Yes\n")
            # No validation/test CSV exists: training EDA must still work.
            manifest = {"source_sha256": digest(root / "raw.csv"), "membership_sha256": digest(root / "memberships.csv"),
                        "splits": {"train": {"sha256": digest(root / "train.csv"), "rows": 1}}}
            (root / "manifest.json").write_text(json.dumps(manifest))
            config = SimpleNamespace(raw_path=root / "raw.csv", output_dir=root)
            exports = Exports(root, "test")
            with patch("opsyntra.notebook_support.load_config", return_value=config):
                frame, _ = verified_partition(root, "train", exports)
                self.assertTrue(frame.TotalCharges.isna().all())
                self.assertEqual(frame.customerID.tolist(), ["A"])
                (root / "train.csv").write_text("customerID,TotalCharges,Churn\nCHANGED,0,No\n")
                with self.assertRaisesRegex(ValueError, "checksum"):
                    verified_partition(root, "train", exports)
            with self.assertRaisesRegex(ValueError, "train/validation"):
                verified_partition(root, "test", exports)

    def test_portable_report_paths_cannot_escape_backend(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.assertEqual(portable_path(root, "candidates\\model.joblib"), root / "candidates/model.joblib")
            with self.assertRaises(ValueError): portable_path(root, "../outside")

    def test_analytics_ranges_fail_before_connecting_and_errors_hide_credentials(self):
        with patch("opsyntra.analytics.config.AnalyticsConfig.from_env", side_effect=AssertionError("must not connect")):
            for kwargs in ({"engine": "all"}, {"hours": 745}, {"as_of": "2026-10-20T10:00:00"}):
                with self.assertRaises(ValueError): analytics_snapshot(**kwargs)
        config = SimpleNamespace(connect=lambda role: (_ for _ in ()).throw(RuntimeError("secret-password")))
        with self.assertRaisesRegex(RuntimeError, "snapshot unavailable") as caught:
            analytics_snapshot(config=config)
        self.assertNotIn("secret-password", str(caught.exception))

