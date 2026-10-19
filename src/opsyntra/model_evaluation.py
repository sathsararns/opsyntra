"""Consistent positive-class churn metrics for model comparison."""

from sklearn.metrics import (
    accuracy_score, average_precision_score, confusion_matrix,
    f1_score, precision_score, recall_score, roc_auc_score,
)


def evaluate_model(pipeline, X, y) -> dict:
    predictions = pipeline.predict(X)
    probabilities = pipeline.predict_proba(X)[:, 1]
    return {
        "accuracy": float(accuracy_score(y, predictions)),
        "precision": float(precision_score(y, predictions, zero_division=0)),
        "recall": float(recall_score(y, predictions, zero_division=0)),
        "f1": float(f1_score(y, predictions, zero_division=0)),
        "roc_auc": float(roc_auc_score(y, probabilities)),
        "average_precision": float(average_precision_score(y, probabilities)),
        "confusion_matrix": confusion_matrix(y, predictions, labels=[0, 1]).tolist(),
    }
