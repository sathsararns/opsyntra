"""CLI for one JSON customer or a batch CSV without requiring Churn."""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from opsyntra.config import load_config, load_training_config
from opsyntra.model_inference import load_pipeline, predict_records


def main() -> int:
    parser = argparse.ArgumentParser(description="Predict raw Telco records using a saved complete pipeline.")
    parser.add_argument("--config", type=Path, default=ROOT / "config.yaml")
    parser.add_argument("--model", type=Path, help="Override the configured best_pipeline.joblib path.")
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--record", type=Path, help="JSON file containing one customer object.")
    inputs.add_argument("--input", type=Path, help="CSV containing customer records.")
    parser.add_argument("--output", type=Path, help="Output CSV for batch input, JSON for one record.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        model_path = args.model or load_training_config(args.config).artifact_dir / "best_pipeline.joblib"
        if args.record:
            data = json.loads(args.record.read_text(encoding="utf-8-sig"))
            if not isinstance(data, dict):
                raise ValueError("--record must contain one JSON customer object.")
        else:
            if not args.output:
                raise ValueError("--input requires --output for batch results.")
            data = pd.read_csv(args.input, dtype="string", keep_default_na=False)
        if args.output:
            resolved_output = args.output.resolve()
            # A typo must not overwrite a model, input, configuration or data artifact.
            data_config = load_config(args.config)
            training_config = load_training_config(args.config)
            protected = [
                args.config, model_path, args.input, args.record,
                data_config.raw_path, data_config.normalized_path,
                data_config.report_path, training_config.report_path, training_config.comparison_path,
                *(data_config.output_dir / filename for filename in (
                    "memberships.csv", "manifest.json", "train.csv", "validation.csv", "test.csv",
                )),
            ]
            if any(path is not None and resolved_output == Path(path).resolve() for path in protected):
                raise ValueError("Prediction output must differ from input, model, configuration and data artifacts.")
        result = predict_records(load_pipeline(model_path), data)
        if args.record:
            content = result.to_json(orient="records", double_precision=15)
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(json.loads(content)[0], indent=2) + "\n", encoding="utf-8")
            print(json.dumps(json.loads(content)[0], indent=2))
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            result.to_csv(args.output, index=False, lineterminator="\n")
            logging.info("Saved %s predictions to %s", len(result), args.output)
    except (ValueError, OSError, KeyError, ImportError) as error:
        logging.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
