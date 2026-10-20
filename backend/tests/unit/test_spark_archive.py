"""Native model archival must retain deep Windows paths and file contents."""
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from opsyntra.orchestration_models import filesystem_path, model_digest
from opsyntra.spark_tracking import archive_pipeline


class SparkArchiveTests(unittest.TestCase):
    def test_archive_retains_deep_native_model_files_and_their_digest(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / "pipeline"
            normal = filesystem_path(source / "metadata/part-00000")
            normal.parent.mkdir(parents=True)
            normal.write_bytes(b'{"class":"PipelineModel"}')
            deep = source / "stages" / ("RandomForestClassifier_" + "a" * 90) / ("nested_" + "b" * 90) / "data" / ("part-" + "c" * 90 + ".parquet")
            deep = filesystem_path(deep)
            deep.parent.mkdir(parents=True)
            deep.write_bytes(b"native parquet fixture")
            archive = Path(folder) / "complete_pipeline.zip"
            result = archive_pipeline(source, archive)
            self.assertEqual(result["files"], 2)
            self.assertEqual(result["native_pipeline_sha256"], model_digest(source))
            with ZipFile(archive) as bundle:
                self.assertEqual(len(bundle.namelist()), 2)
                self.assertIn(b"native parquet fixture", [bundle.read(n) for n in bundle.namelist()])
            # Cleanup through extended paths on Windows as well.
            import shutil
            cleanup = filesystem_path(source)
            self.assertTrue(cleanup.is_relative_to(filesystem_path(Path(folder))))
            shutil.rmtree(cleanup)

    def test_archive_cannot_be_written_inside_its_source_model(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder)
            (source / "metadata").write_text("model")
            with self.assertRaisesRegex(ValueError, "outside"):
                archive_pipeline(source, source / "recursive.zip")
