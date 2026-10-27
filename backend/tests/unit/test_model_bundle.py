"""A saved-model bundle must reject altered or unsafe payloads before extraction."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from tools.evidence.model_bundle import verify, restore


class ModelBundleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "models.zip"

    def bundle(self, name="artifacts/models/best_pipeline.joblib", changed=False, duplicate=False):
        data = b"fixture model bytes"
        manifest = {"format_version": 1, "members": [{"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}]}
        with ZipFile(self.path, "w") as archive:
            archive.writestr("bundle_manifest.json", json.dumps(manifest))
            archive.writestr(name, b"altered" if changed else data)
            if duplicate:
                archive.writestr(name.upper(), data)

    def test_verified_bundle_has_payload_and_archive_checksums(self):
        self.bundle()
        result = verify(self.path)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["bundle_sha256"], hashlib.sha256(self.path.read_bytes()).hexdigest())

    def test_altered_payload_is_rejected(self):
        self.bundle(changed=True)
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            verify(self.path)

    def test_traversal_unexpected_files_and_case_collisions_are_rejected(self):
        for name in ("../consumer.py", "/absolute/model", "artifacts/models/../../consumer.py", "src/opsyntra/api/app.py"):
            with self.subTest(name=name):
                self.bundle(name)
                with self.assertRaisesRegex(ValueError, "unsafe"):
                    verify(self.path)
        self.bundle(duplicate=True)
        with self.assertRaisesRegex(ValueError, "unsafe"):
            verify(self.path)

    def destination(self):
        import yaml
        root = self.path.parent / "checkout"
        root.mkdir()
        settings = yaml.safe_load((Path(__file__).resolve().parents[2] / "config.yaml").read_text())
        (root / "config.yaml").write_text(yaml.safe_dump(settings))
        raw = root / settings["data"]["raw_path"]
        raw.parent.mkdir(parents=True)
        raw.write_bytes(b"fixture raw data")
        members = root / settings["splitting"]["output_dir"] / "memberships.csv"
        members.parent.mkdir(parents=True)
        members.write_bytes(b"fixture memberships")
        with ZipFile(self.path) as archive:
            manifest = json.loads(archive.read("bundle_manifest.json"))
            content = archive.read(manifest["members"][0]["path"])
        manifest.update(dataset_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(), membership_sha256=hashlib.sha256(members.read_bytes()).hexdigest())
        with ZipFile(self.path, "w") as archive:
            archive.writestr("bundle_manifest.json", json.dumps(manifest))
            archive.writestr(manifest["members"][0]["path"], content)
        return root, raw

    def test_restore_is_idempotent_and_refuses_conflicting_models(self):
        self.bundle()
        root, _ = self.destination()
        restore(root, self.path)
        restore(root, self.path)
        model = root / "artifacts/models/best_pipeline.joblib"
        self.assertEqual(model.read_bytes(), b"fixture model bytes")
        model.write_bytes(b"another fitted model")
        with self.assertRaisesRegex(ValueError, "Different model"):
            restore(root, self.path)
        self.assertEqual(model.read_bytes(), b"another fitted model")

    def test_restore_rejects_dataset_change_before_writing_any_model(self):
        self.bundle()
        root, raw = self.destination()
        raw.write_bytes(b"different customers")
        with self.assertRaisesRegex(ValueError, "dataset/membership"):
            restore(root, self.path)
        self.assertFalse((root / "artifacts/models/best_pipeline.joblib").exists())
