"""Consistent positive-class churn metrics for model comparison."""

import numpy as np
from sklearn.metrics import (
    accuracy_score, average_precision_score, confusion_matrix,
    f1_score, precision_score, recall_score, roc_auc_score, precision_recall_curve,
)


def select_threshold(y, probabilities) -> dict:
    """Maximize validation F1; ties prefer proximity to 0.5, then higher threshold."""
    precision, recall, thresholds = precision_recall_curve(y, probabilities)
    scores = np.divide(2 * precision[:-1] * recall[:-1], precision[:-1] + recall[:-1],
                       out=np.zeros_like(thresholds), where=(precision[:-1] + recall[:-1]) > 0)
    index = max(range(len(thresholds)), key=lambda i: (scores[i], -abs(thresholds[i] - .5), thresholds[i]))
    return {"threshold": float(thresholds[index]), "f1": float(scores[index]),
            "precision": float(precision[index]), "recall": float(recall[index]),
            "thresholds": thresholds.tolist(), "f1_scores": scores.tolist()}


def evaluate_model(pipeline, X, y) -> dict:
    predictions = pipeline.predict(X)
    probabilities = pipeline.predict_proba(X)[:, 1]
    return evaluate_probabilities(y, probabilities, predictions=predictions)


def evaluate_probabilities(y, probabilities, threshold=.5, *, predictions=None) -> dict:
    """Identical metric definitions for JVM Spark scores and sklearn scores."""
    probabilities = np.asarray(probabilities, dtype=float)
    if predictions is None:
        predictions = (probabilities >= threshold).astype(int)
    return {
        "accuracy": float(accuracy_score(y, predictions)),
        "precision": float(precision_score(y, predictions, zero_division=0)),
        "recall": float(recall_score(y, predictions, zero_division=0)),
        "f1": float(f1_score(y, predictions, zero_division=0)),
        "roc_auc": float(roc_auc_score(y, probabilities)),
        "average_precision": float(average_precision_score(y, probabilities)),
        "confusion_matrix": confusion_matrix(y, predictions, labels=[0, 1]).tolist(),
    }
