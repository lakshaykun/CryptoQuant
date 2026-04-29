import numpy as np
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    recall_score,
)

def _safe_float(value):
    if value is None:
        return 0.0

    value = float(value)
    if np.isnan(value) or np.isinf(value):
        return 0.0
    return value


def evaluate_regression(y_true, y_pred) -> dict:
    y_true = np.asarray(y_true).reshape(-1).astype(float)
    y_pred = np.asarray(y_pred).reshape(-1).astype(float)
    correlation = np.corrcoef(y_true, y_pred)[0, 1] if len(y_true) > 1 else 0.0
    return {
        "rmse": _safe_float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": _safe_float(mean_absolute_error(y_true, y_pred)),
        "correlation": _safe_float(correlation),
    }


def evaluate_classification(y_true, y_pred) -> dict:
    y_true = np.asarray(y_true).reshape(-1).astype(int)
    y_pred = np.asarray(y_pred).reshape(-1).astype(int)
    labels = sorted(np.unique(np.concatenate([y_true, y_pred])).tolist())
    return {
        "accuracy": _safe_float(accuracy_score(y_true, y_pred)),
        "f1": _safe_float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "precision": _safe_float(precision_score(y_true, y_pred, average="macro", zero_division=0)),
        "recall": _safe_float(recall_score(y_true, y_pred, average="macro", zero_division=0)),
        "confusion_matrix": confusion_matrix(y_true, y_pred, labels=labels).tolist(),
    }