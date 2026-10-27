"""Resolve historical model class paths without duplicating application modules.

Joblib and Spark metadata refer to the old flat module names. Keep those imports
as lazy aliases so existing approved artifacts still load without retraining.
New application code uses the grouped packages directly. Importing Opsyntra does
not import pandas, MLflow, Kafka or PySpark.
"""
from importlib import import_module
from importlib.abc import Loader, MetaPathFinder
from importlib.util import spec_from_loader
import sys

LEGACY_MODULES = {
    "data_validation": "data.validation",
    "data_splitting": "data.splitting",
    "data_pipeline": "data.pipeline",
    "training_data": "data.training",
    "preprocessing_sklearn": "ml.preprocessing",
    "model_building": "ml.building",
    "model_training": "ml.training",
    "model_evaluation": "ml.evaluation",
    "model_inference": "ml.inference",
    "experiment_tracking": "ml.tracking",
    "engine_comparison": "ml.comparison",
    "notebook_support": "ml.notebooks",
    "spark_runtime": "spark.runtime",
    "spark_pipeline": "spark.pipeline",
    "spark_training": "spark.training",
    "spark_inference": "spark.inference",
    "spark_tracking": "spark.tracking",
    "kafka_config": "kafka.config",
    "kafka_events": "kafka.events",
    "kafka_models": "kafka.models",
    "kafka_publishing": "kafka.publishing",
    "kafka_consumption": "kafka.consumption",
    "kafka_recovery": "kafka.recovery",
    "airflow_workflow": "orchestration.workflow",
    "orchestration_models": "orchestration.models",
    "kafka_workflow": "orchestration.kafka",
    "web_api": "api.app",
}


class _LegacyImports(MetaPathFinder, Loader):
    def find_spec(self, fullname, path=None, target=None):
        prefix, _, name = fullname.rpartition(".")
        if prefix == "opsyntra" and name in LEGACY_MODULES:
            return spec_from_loader(fullname, self)
        return None

    def create_module(self, spec):
        name = spec.name.rpartition(".")[2]
        return import_module("opsyntra." + LEGACY_MODULES[name])

    def exec_module(self, module):
        # create_module returns the already initialized canonical module.
        pass


def install_legacy_imports():
    if not any(isinstance(finder, _LegacyImports) for finder in sys.meta_path):
        sys.meta_path.insert(0, _LegacyImports())
