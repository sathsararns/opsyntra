"""One complete sklearn Pipeline for each requested classifier."""

from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from .preprocessing_sklearn import NormalizeTelcoInputs, build_preprocessor


def build_model_pipeline(name: str, *, seed: int = 42, jobs: int = 2) -> Pipeline:
    if name == "dummy":
        classifier = DummyClassifier(strategy="most_frequent", random_state=seed)
    elif name == "logistic_regression":
        classifier = LogisticRegression(max_iter=2000, random_state=seed)
    elif name == "random_forest":
        classifier = RandomForestClassifier(n_estimators=200, random_state=seed, n_jobs=jobs)
    elif name == "xgboost":
        try:
            from xgboost import XGBClassifier
        except ImportError as error:
            raise ImportError("Install the ML dependencies: python -m pip install -r requirements/ml.txt") from error
        classifier = XGBClassifier(
            n_estimators=200, max_depth=3, learning_rate=.05,
            objective="binary:logistic", eval_metric="logloss", tree_method="hist",
            random_state=seed, n_jobs=jobs,
        )
    else:
        raise ValueError(f"Unknown model: {name}")
    return Pipeline([
        ("normalize", NormalizeTelcoInputs()),
        ("preprocessing", build_preprocessor(scale_numeric=name == "logistic_regression")),
        ("classifier", classifier),
    ])
