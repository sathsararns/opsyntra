"""CLI: Spark single-customer JSON or CSV batch inference without Churn."""

import argparse
import json
import logging
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--model", type=Path)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--record", type=Path)
    inputs.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if sys.platform == "win32":
        parser.error("Use ..\\start-spark.ps1 -Task predict -Record examples/customer.json from backend. Spark runs in Ubuntu WSL.")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from opsyntra.config import load_config, load_training_config
    from opsyntra.spark_training import load_spark_settings
    from opsyntra.spark_runtime import create_spark
    from opsyntra.spark_inference import load_spark_pipeline, predict_spark_frame
    config_path = args.config.resolve()
    root = config_path.parent
    settings = load_spark_settings(config_path)
    if not args.record and not args.output:
        parser.error("--input requires --output")
    if args.model:
        model_path = args.model.resolve()
    else:
        selected = json.loads((root / settings["artifact_dir"] / "selected_model.json").read_text(encoding="utf-8"))
        model_path = root / selected["pipeline_path"]
    if args.output:
        target = args.output.resolve()
        data_config, training_config = load_config(config_path), load_training_config(config_path)
        protected = [config_path, args.record, args.input, data_config.raw_path, data_config.normalized_path,
                     data_config.report_path, training_config.report_path, training_config.comparison_path,
                     root / settings["report_path"], root / settings["comparison_path"]]
        if (target.is_relative_to((root / settings["artifact_dir"]).resolve()) or
                target.is_relative_to(training_config.artifact_dir) or target.is_relative_to(data_config.output_dir) or
                target.is_relative_to(model_path.resolve()) or
                any(p is not None and target == Path(p).resolve() for p in protected)):
            raise ValueError("Prediction output must differ from input, model, configuration and data artifacts.")
    spark = create_spark(settings, root)
    try:
        if args.record:
            record = json.loads(args.record.read_text(encoding="utf-8-sig"))
            if not isinstance(record, dict):
                raise ValueError("--record requires one JSON object.")
            with tempfile.TemporaryDirectory(dir=root / ".spark-runtime") as temporary:
                input_path = Path(temporary) / "record.csv"
                pd.DataFrame([record]).to_csv(input_path, index=False)
                frame = spark.read.option("header", True).csv(input_path.as_uri())
                result = predict_spark_frame(load_spark_pipeline(model_path), frame)
            content = json.dumps(json.loads(result.to_json(orient="records", double_precision=15))[0], indent=2) + "\n"
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(content, encoding="utf-8")
            print(content)
        else:
            frame = spark.read.option("header", True).csv(args.input.resolve().as_uri())
            result = predict_spark_frame(load_spark_pipeline(model_path), frame)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            result.to_csv(args.output, index=False, lineterminator="\n")
            logging.info("Saved %s Spark predictions to %s", len(result), args.output)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
