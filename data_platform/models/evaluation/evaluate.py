import numpy as np
from sklearn.metrics import (
    explained_variance_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
)

from models.evaluation.backtesting import backtest


def _safe_float(value):
    if value is None:
        return 0.0

    value = float(value)
    if np.isnan(value) or np.isinf(value):
        return 0.0
    return value


def evaluate_model(
    model,
    X_test,
    y_test,
    threshold=0.0,
    transaction_cost=0.0,
) -> tuple[dict, object]:
    preds = np.asarray(model.predict(X_test)).reshape(-1)
    y_true = np.asarray(y_test).reshape(-1)

    results = {
        "rmse": _safe_float(np.sqrt(mean_squared_error(y_true, preds))),
        "mae": _safe_float(mean_absolute_error(y_true, preds)),
        "r2": _safe_float(r2_score(y_true, preds)),
        "explained_variance": _safe_float(explained_variance_score(y_true, preds)),
        "prediction_bias": _safe_float(np.mean(preds - y_true)),
    }

    actual_signal = np.select(
        [y_true > threshold, y_true < -threshold],
        [1, -1],
        default=0,
    )
    predicted_signal = np.select(
        [preds > threshold, preds < -threshold],
        [1, -1],
        default=0,
    )

    results["directional_accuracy"] = _safe_float(np.mean(predicted_signal == actual_signal))

    actual_long = (actual_signal == 1).astype(int)
    predicted_long = (predicted_signal == 1).astype(int)
    actual_short = (actual_signal == -1).astype(int)
    predicted_short = (predicted_signal == -1).astype(int)

    results["long_precision"] = _safe_float(
        precision_score(actual_long, predicted_long, zero_division=0)
    )
    results["long_recall"] = _safe_float(
        recall_score(actual_long, predicted_long, zero_division=0)
    )
    results["long_f1"] = _safe_float(f1_score(actual_long, predicted_long, zero_division=0))

    results["short_precision"] = _safe_float(
        precision_score(actual_short, predicted_short, zero_division=0)
    )
    results["short_recall"] = _safe_float(
        recall_score(actual_short, predicted_short, zero_division=0)
    )
    results["short_f1"] = _safe_float(f1_score(actual_short, predicted_short, zero_division=0))

    results["flat_rate"] = _safe_float((predicted_signal == 0).mean())

    if len(y_true) > 1:
        correlation = np.corrcoef(y_true, preds)[0, 1]
    else:
        correlation = 0.0
    results["prediction_correlation"] = _safe_float(correlation)

    report = backtest(y_true, preds, threshold=threshold, transaction_cost=transaction_cost)
    strategy_returns = report["strategy_return"].astype(float)
    benchmark_returns = report["benchmark_return"].astype(float)

    strategy_cumulative = report["strategy_cumulative_return"].iloc[-1] if not report.empty else 1.0
    benchmark_cumulative = report["benchmark_cumulative_return"].iloc[-1] if not report.empty else 1.0

    strategy_std = strategy_returns.std(ddof=0)
    downside_returns = strategy_returns[strategy_returns < 0]
    downside_std = downside_returns.std(ddof=0) if not downside_returns.empty else 0.0

    results["strategy_total_return"] = _safe_float(strategy_cumulative - 1)
    results["benchmark_total_return"] = _safe_float(benchmark_cumulative - 1)
    results["excess_return"] = _safe_float(results["strategy_total_return"] - results["benchmark_total_return"])
    results["strategy_sharpe_ratio"] = _safe_float(strategy_returns.mean() / (strategy_std + 1e-10))
    results["strategy_sortino_ratio"] = _safe_float(strategy_returns.mean() / (downside_std + 1e-10))
    results["max_drawdown"] = _safe_float(report["strategy_drawdown"].min())
    results["win_rate"] = _safe_float((strategy_returns > 0).mean())
    results["profit_factor"] = _safe_float(
        strategy_returns[strategy_returns > 0].sum()
        / (abs(strategy_returns[strategy_returns < 0].sum()) + 1e-10)
    )
    results["signal_coverage"] = _safe_float((report["signal"] != 0).mean())
    results["average_turnover"] = _safe_float(report["turnover"].mean())
    results["benchmark_mean_return"] = _safe_float(benchmark_returns.mean())

    return results, report