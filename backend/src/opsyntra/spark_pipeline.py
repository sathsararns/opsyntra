"""Persistable Spark stages accepting the same raw Telco records as sklearn."""

from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import functions as F

from .data_validation import FEATURE_COLUMNS, NUMERIC_COLUMNS

CATEGORICAL_COLUMNS = tuple(c for c in FEATURE_COLUMNS if c not in NUMERIC_COLUMNS)


class NormalizeSparkTelco(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    """SQL-only normalization; invalid numbers fail when Spark executes the plan."""

    def _transform(self, dataset):
        missing = sorted(set(FEATURE_COLUMNS) - set(dataset.columns))
        if missing:
            raise ValueError(f"Missing required predictor columns: {missing}")
        if len(dataset.columns) != len(set(dataset.columns)):
            raise ValueError("Input column names must be unique.")
        clean = dataset
        for column in FEATURE_COLUMNS:
            raw = F.trim(F.col(column).cast("string"))
            if column in NUMERIC_COLUMNS:
                value = raw.try_cast("double")
                valid = value.isNotNull() & ~F.isnan(value) & (F.abs(value) != float("inf")) & (value >= 0)
                if column in ("SeniorCitizen", "tenure"):
                    valid = valid & (value == F.floor(value))
                if column == "SeniorCitizen":
                    valid = valid & value.isin(0., 1.)
                value = (F.when(raw.isNull() | (raw == ""), F.lit(None).cast("double"))
                         .when(valid, value)
                         .otherwise(F.raise_error(F.lit(f"{column}: invalid numeric value")).cast("double")))
            else:
                value = F.when(raw.isNull() | (raw == ""), "__MISSING__").otherwise(raw)
            clean = clean.withColumn(column, value)
        if "customerID" in clean.columns:
            clean = clean.withColumn("customerID", F.trim(F.col("customerID").cast("string")))
        return clean


class SparkChurnDecision(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    threshold = Param(Params._dummy(), "threshold", "Validation F1 operating point", TypeConverters.toFloat)
    modelVersion = Param(Params._dummy(), "modelVersion", "Opsyntra model version", TypeConverters.toString)

    def __init__(self, threshold=.5, modelVersion="unversioned"):
        super().__init__()
        self._setDefault(threshold=.5, modelVersion="unversioned")
        self._set(threshold=threshold, modelVersion=modelVersion)

    def _transform(self, dataset):
        return (dataset.withColumn("churn_probability", vector_to_array("probability")[1])
                .withColumn("prediction", F.when(F.col("churn_probability") >= self.getOrDefault(self.threshold), "Yes").otherwise("No"))
                .withColumn("model_version", F.lit(self.getOrDefault(self.modelVersion))))


def build_spark_preprocessing(name):
    numeric = [f"_num_{i}" for i in range(len(NUMERIC_COLUMNS))]
    indexed = [f"_idx_{i}" for i in range(len(CATEGORICAL_COLUMNS))]
    encoded = [f"_cat_{i}" for i in range(len(CATEGORICAL_COLUMNS))]
    stages = [
        NormalizeSparkTelco(),
        Imputer(inputCols=list(NUMERIC_COLUMNS), outputCols=numeric, strategy="median", relativeError=0.),
        StringIndexer(inputCols=list(CATEGORICAL_COLUMNS), outputCols=indexed,
                      stringOrderType="alphabetAsc", handleInvalid="keep"),
        # Indexer's extra unknown bucket is last. Dropping it gives all-zero
        # unknown categories, matching sklearn's handle_unknown='ignore'.
        OneHotEncoder(inputCols=indexed, outputCols=encoded, dropLast=True, handleInvalid="error"),
    ]
    if name == "logistic_regression":
        stages += [VectorAssembler(inputCols=numeric, outputCol="_numeric"),
                   StandardScaler(inputCol="_numeric", outputCol="_scaled_numeric", withMean=True, withStd=True)]
        numeric_inputs = ["_scaled_numeric"]
    else:
        numeric_inputs = numeric
    stages.append(VectorAssembler(inputCols=numeric_inputs + encoded, outputCol="features", handleInvalid="error"))
    return Pipeline(stages=stages)


def build_spark_classifier(name, parameters, seed):
    if name == "logistic_regression":
        return LogisticRegression(labelCol="label", featuresCol="features", predictionCol="native_prediction", **parameters)
    if name == "random_forest":
        return RandomForestClassifier(labelCol="label", featuresCol="features", predictionCol="native_prediction", seed=seed, **parameters)
    if name == "xgboost":
        from xgboost.spark import SparkXGBClassifier
        return SparkXGBClassifier(label_col="label", features_col="features", prediction_col="native_prediction",
                                  probability_col="probability", random_state=seed, device="cpu", **parameters)
    raise ValueError(f"Unsupported Spark model: {name}")
