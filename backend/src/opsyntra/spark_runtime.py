"""A bounded local Spark session with an explicit worker Python executable."""

import os
import sys
from pathlib import Path


def create_spark(settings, root: Path):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    runtime = root / ".spark-runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    builder = (SparkSession.builder.master(settings["master"]).appName("Opsyntra-Telco")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", str(settings["shuffle_partitions"]))
               .config("spark.default.parallelism", str(settings["shuffle_partitions"]))
               .config("spark.task.cpus", "1")
               .config("spark.pyspark.python", sys.executable)
               .config("spark.driver.host", "127.0.0.1")
               .config("spark.local.dir", str(runtime))
               .config("spark.sql.warehouse.dir", (runtime / "warehouse").as_uri()))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
