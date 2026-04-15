import numpy as np
from sklearn.metrics import mean_squared_error

def evaluate_model(model, X_test, y_test) -> dict:
    preds = model.predict(X_test)

    results = {}

    results["rmse"] = np.sqrt(mean_squared_error(y_test, preds))

    # Directional accuracy (VERY IMPORTANT)
    direction_acc = np.mean((preds > 0) == (y_test > 0))
    results["directional_accuracy"] = direction_acc

    results["precision"] = np.mean((preds > 0) & (y_test > 0)) / (np.mean(preds > 0) + 1e-10)

    return results