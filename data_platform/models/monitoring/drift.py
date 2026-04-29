import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import requests
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from utils_global.config_loader import load_config
from utils_global.logger import get_logger


logger = get_logger(__name__)


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()


def _ks_statistic(expected: pd.Series, actual: pd.Series) -> float:
    expected_values = np.sort(_safe_numeric(expected).to_numpy())
    actual_values = np.sort(_safe_numeric(actual).to_numpy())

    if expected_values.size < 2 or actual_values.size < 2:
        return 0.0

    sample = np.sort(np.unique(np.concatenate([expected_values, actual_values])))
    if sample.size < 2:
        return 0.0

    expected_cdf = np.searchsorted(expected_values, sample, side="right") / expected_values.size
    actual_cdf = np.searchsorted(actual_values, sample, side="right") / actual_values.size
    return float(np.max(np.abs(expected_cdf - actual_cdf)))


def _mean_std_shift(expected: pd.Series, actual: pd.Series) -> Tuple[float, float, float]:
    expected_values = _safe_numeric(expected)
    actual_values = _safe_numeric(actual)

    if expected_values.size < 2 or actual_values.size < 2:
        return 0.0, 0.0, 0.0

    expected_mean = float(expected_values.mean())
    actual_mean = float(actual_values.mean())
    expected_std = float(expected_values.std(ddof=0))
    actual_std = float(actual_values.std(ddof=0))

    mean_shift = abs(actual_mean - expected_mean) / (abs(expected_mean) + 1e-6)
    std_shift = abs(actual_std - expected_std) / (abs(expected_std) + 1e-6)
    score = 0.5 * mean_shift + 0.5 * std_shift
    return float(score), float(mean_shift), float(std_shift)


def _resolve_monitoring_config() -> Dict:
    model_config = load_config("configs/model.yaml") or {}

    monitoring_cfg = model_config.get("monitoring") or {}
    drift_cfg = monitoring_cfg.get("drift") or {}
    retraining_cfg = monitoring_cfg.get("retraining") or {}
    history_cfg = monitoring_cfg.get("history") or {}

    return {
        "model": model_config,
        "drift": {
            "recent_window": int(drift_cfg.get("recent_window", 5000)),
            "baseline_window": int(drift_cfg.get("baseline_window", 20000)),
            "min_rows": int(drift_cfg.get("min_rows", 200)),
            "data_drift_threshold": float(drift_cfg.get("data_drift_threshold", 0.2)),
            "model_drift_threshold": float(
                drift_cfg.get("model_drift_threshold", drift_cfg.get("prediction_drift_threshold", 0.25))
            ),
        },
        "retraining": {
            "airflow_api_url": retraining_cfg.get(
                "airflow_api_url", "http://airflow-webserver:8080/api/v1"
            ),
            "dag_id": retraining_cfg.get("dag_id", "model_training_pipeline"),
            "username": os.getenv(
                "AIRFLOW_API_USERNAME", retraining_cfg.get("username", "airflow")
            ),
            "password": os.getenv(
                "AIRFLOW_API_PASSWORD", retraining_cfg.get("password", "airflow")
            ),
            "cooldown_minutes": int(retraining_cfg.get("cooldown_minutes", 120)),
            "state_path": retraining_cfg.get(
                "state_path", "/opt/app/delta/state/monitoring/retraining_state.json"
            ),
        },
        "history": {
            "path": history_cfg.get("path", "/opt/app/delta/state/monitoring/drift_history"),
        },
    }


def _load_gold_recent_frame(columns: List[str]) -> pd.DataFrame:
    data_cfg = load_config("configs/data.yaml")
    gold_path = data_cfg["tables"]["gold_market"]["path"]
    table = DeltaTable(gold_path)
    return table.to_pandas(columns=columns)


def _load_predictions_frame(columns: List[str]) -> pd.DataFrame:
    data_cfg = load_config("configs/data.yaml")
    predictions_path = data_cfg["tables"]["predictions_log_return_lead1"]["path"]
    table = DeltaTable(predictions_path)
    return table.to_pandas(columns=columns)


def _filter_drift_features(features: List[str]) -> List[str]:
    exclude = {
        "symbol",
        "open_time",
        "date",
        "ingestion_time",
        "is_valid_feature_row",
        "open",
        "high",
        "low",
        "close",
        "hour",
        "day_of_week",
    }
    return [feature for feature in features if feature not in exclude]


def _compute_data_drift_rows(
    baseline_df: pd.DataFrame,
    recent_df: pd.DataFrame,
    features: List[str],
    cfg: Dict,
) -> Tuple[List[Dict], Dict]:
    rows: List[Dict] = []
    scores: List[float] = []

    for feature in features:
        if feature not in baseline_df.columns or feature not in recent_df.columns:
            continue

        baseline_series = _safe_numeric(baseline_df[feature])
        recent_series = _safe_numeric(recent_df[feature])

        if baseline_series.size < cfg["min_rows"] or recent_series.size < cfg["min_rows"]:
            continue

        ks_value = _ks_statistic(baseline_series, recent_series)
        mean_std_score, mean_shift, std_shift = _mean_std_shift(baseline_series, recent_series)
        drift_score = max(ks_value, mean_std_score)
        drift_detected = bool(drift_score >= cfg["data_drift_threshold"])

        rows.append(
            {
                "feature_name": feature,
                "drift_type": "data",
                "drift_score": float(drift_score),
                "drift_detected": drift_detected,
                "model_metric": None,
                "threshold": float(cfg["data_drift_threshold"]),
                "ks_statistic": float(ks_value),
                "mean_shift": float(mean_shift),
                "std_shift": float(std_shift),
            }
        )
        scores.append(float(drift_score))

    summary = {
        "drift_score": float(max(scores)) if scores else 0.0,
        "drift_detected": bool(any(row["drift_detected"] for row in rows)),
        "evaluated_features": int(len(rows)),
    }
    return rows, summary


def _prepare_prediction_frame(predictions_df: pd.DataFrame) -> pd.DataFrame:
    if predictions_df.empty:
        return predictions_df

    prepared = predictions_df.copy()
    prepared["open_time"] = pd.to_datetime(prepared["open_time"], errors="coerce")
    prepared = prepared.sort_values(["symbol", "open_time"])
    prepared["actual_log_return_lead1"] = prepared.groupby("symbol")["log_return"].shift(-1)
    prepared["prediction"] = pd.to_numeric(prepared["prediction"], errors="coerce")
    prepared["actual_log_return_lead1"] = pd.to_numeric(
        prepared["actual_log_return_lead1"], errors="coerce"
    )
    return prepared.dropna(subset=["prediction", "actual_log_return_lead1"])


def _compute_model_drift(
    predictions_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    target_columns: List[str],
    cfg: Dict,
) -> Dict:
    if predictions_df.empty:
        return {"drift_score": 0.0, "drift_detected": False, "per_target": {}}

    recent = predictions_df.tail(cfg["recent_window"]).copy()
    per_target = {}
    scores = []
    detected = False

    for target in target_columns:
        if target not in recent.columns or target not in baseline_df.columns:
            continue
        baseline_series = _safe_numeric(baseline_df[target].tail(cfg["baseline_window"]))
        recent_series = _safe_numeric(recent[target])
        if baseline_series.size < cfg["min_rows"] or recent_series.size < cfg["min_rows"]:
            continue
        ks_value = _ks_statistic(baseline_series, recent_series)
        mean_std_score, mean_shift, std_shift = _mean_std_shift(baseline_series, recent_series)
        drift_score = max(float(ks_value), float(mean_std_score))
        is_detected = drift_score >= cfg["model_drift_threshold"]
        per_target[target] = {
            "drift_score": drift_score,
            "drift_detected": bool(is_detected),
            "ks_error": float(ks_value),
            "mean_shift": float(mean_shift),
            "std_shift": float(std_shift),
        }
        scores.append(drift_score)
        detected = detected or is_detected

    return {
        "drift_score": float(max(scores)) if scores else 0.0,
        "drift_detected": bool(detected),
        "per_target": per_target,
    }


def evaluate_drift() -> Dict:
    resolved_cfg = _resolve_monitoring_config()
    model_cfg = resolved_cfg["model"]
    drift_cfg = resolved_cfg["drift"]

    raw_features = model_cfg.get("features_long", model_cfg.get("features_short", model_cfg.get("features", [])))
    features = _filter_drift_features(raw_features)

    training_path = model_cfg.get("train_data_path")
    if not training_path or not Path(training_path).exists():
        logger.warning(
            "Training baseline dataset not found at '%s'. Data drift score will remain 0.0 until baseline exists.",
            training_path,
        )
        baseline_df = pd.DataFrame(columns=features)
    else:
        baseline_df = pd.read_parquet(training_path)

    gold_columns = sorted(set(features + ["open_time", "symbol"]))
    try:
        gold_df = _load_gold_recent_frame(columns=gold_columns)
        gold_df["open_time"] = pd.to_datetime(gold_df["open_time"], errors="coerce")
        gold_df = gold_df.sort_values("open_time")
    except Exception as exc:
        logger.warning("Failed to load gold dataset for drift monitoring: %s", exc)
        gold_df = pd.DataFrame(columns=gold_columns)

    recent_df = gold_df.tail(drift_cfg["recent_window"])
    baseline_df = baseline_df.tail(drift_cfg["baseline_window"])
    data_rows, data_summary = _compute_data_drift_rows(baseline_df, recent_df, features, drift_cfg)

    target_columns = list((model_cfg.get("models") or {}).keys()) or ["prediction"]
    predictions_columns = list(dict.fromkeys(["open_time", "symbol", *target_columns, "prediction"]))
    try:
        predictions_df = _load_predictions_frame(columns=predictions_columns)
        predictions_df["open_time"] = pd.to_datetime(predictions_df["open_time"], errors="coerce")
        predictions_df = predictions_df.sort_values("open_time")
    except Exception as exc:
        logger.warning("Failed to load predictions dataset for drift monitoring: %s", exc)
        predictions_df = pd.DataFrame(columns=predictions_columns)

    model_drift = _compute_model_drift(predictions_df, baseline_df, target_columns, drift_cfg)

    overall_score = max(data_summary["drift_score"], model_drift["drift_score"])
    drift_detected = bool(data_summary["drift_detected"] or model_drift["drift_detected"])
    timestamp = datetime.now(timezone.utc)

    model_rows = []
    for target_name, target_metrics in model_drift.get("per_target", {}).items():
        model_rows.append(
            {
                "feature_name": f"__model__.{target_name}",
                "drift_type": "model",
                "drift_score": float(target_metrics["drift_score"]),
                "drift_detected": bool(target_metrics["drift_detected"]),
                "model_metric": float(target_metrics["drift_score"]),
                "threshold": float(drift_cfg["model_drift_threshold"]),
                "ks_statistic": float(target_metrics["ks_error"]),
                "mean_shift": float(target_metrics["mean_shift"]),
                "std_shift": float(target_metrics["std_shift"]),
            }
        )

    model_row = {
        "feature_name": "__model__",
        "drift_type": "model",
        "drift_score": float(model_drift["drift_score"]),
        "drift_detected": bool(model_drift["drift_detected"]),
        "model_metric": float(model_drift["drift_score"]),
        "threshold": float(drift_cfg["model_drift_threshold"]),
        "ks_statistic": 0.0,
        "mean_shift": None,
        "std_shift": None,
    }

    overall_row = {
        "feature_name": "__overall__",
        "drift_type": "overall",
        "drift_score": float(overall_score),
        "drift_detected": drift_detected,
        "model_metric": float(model_drift["drift_score"]),
        "threshold": float(max(drift_cfg["data_drift_threshold"], drift_cfg["model_drift_threshold"])),
        "ks_statistic": 0.0,
        "mean_shift": None,
        "std_shift": None,
        "data_drift_score": float(data_summary["drift_score"]),
        "model_drift_score": float(model_drift["drift_score"]),
    }

    rows = data_rows + model_rows + [model_row, overall_row]

    logger.info(
        "[drift_monitor] timestamp=%s overall=%.4f data=%.4f model=%.4f detected=%s features=%s",
        timestamp.isoformat(),
        overall_score,
        data_summary["drift_score"],
        model_drift["drift_score"],
        drift_detected,
        data_summary["evaluated_features"],
    )

    return {
        "timestamp": timestamp,
        "drift_score": float(overall_score),
        "drift_detected": drift_detected,
        "data_drift": data_summary,
        "model_drift": model_drift,
        "rows": rows,
    }


def _load_retraining_state(path: str) -> Dict:
    state_path = Path(path)
    if not state_path.exists():
        return {}

    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_retraining_state(path: str, state: Dict) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _cooldown_active(last_trigger_at: Optional[str], cooldown_minutes: int) -> bool:
    if not last_trigger_at:
        return False

    try:
        last_trigger_dt = datetime.fromisoformat(last_trigger_at)
        if last_trigger_dt.tzinfo is None:
            last_trigger_dt = last_trigger_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return False

    elapsed_seconds = (datetime.now(timezone.utc) - last_trigger_dt).total_seconds()
    return elapsed_seconds < (cooldown_minutes * 60)


def trigger_retraining(drift_result: Dict) -> Dict:
    resolved_cfg = _resolve_monitoring_config()
    retraining_cfg = resolved_cfg["retraining"]

    if not drift_result.get("drift_detected", False):
        return {"triggered": False, "reason": "no_drift"}

    state = _load_retraining_state(retraining_cfg["state_path"])
    if _cooldown_active(state.get("last_trigger_at"), retraining_cfg["cooldown_minutes"]):
        return {"triggered": False, "reason": "cooldown_active"}

    dag_id = retraining_cfg["dag_id"]
    run_id = f"drift_retrain_{int(datetime.now(timezone.utc).timestamp())}"
    endpoint = f"{retraining_cfg['airflow_api_url'].rstrip('/')}/dags/{dag_id}/dagRuns"

    payload = {
        "dag_run_id": run_id,
        "conf": {
            "trigger_source": "drift_monitor",
            "drift_score": drift_result["drift_score"],
            "data_drift_score": drift_result["data_drift"]["drift_score"],
            "model_drift_score": drift_result["model_drift"]["drift_score"],
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    try:
        response = requests.post(
            endpoint,
            auth=(retraining_cfg["username"], retraining_cfg["password"]),
            json=payload,
            timeout=15,
        )
    except requests.RequestException as exc:
        logger.error("Failed to reach Airflow API endpoint %s: %s", endpoint, exc)
        return {
            "triggered": False,
            "reason": "airflow_api_unreachable",
        }

    if response.status_code not in {200, 201}:
        logger.error("Failed to trigger retraining DAG: status=%s body=%s", response.status_code, response.text)
        return {
            "triggered": False,
            "reason": "airflow_api_error",
            "status_code": response.status_code,
        }

    _save_retraining_state(
        retraining_cfg["state_path"],
        {
            "last_trigger_at": datetime.now(timezone.utc).isoformat(),
            "last_dag_run_id": run_id,
            "last_drift_score": drift_result["drift_score"],
        },
    )

    logger.info("Triggered retraining DAG '%s' via Airflow API with run_id=%s", dag_id, run_id)
    return {
        "triggered": True,
        "reason": "drift_detected",
        "dag_id": dag_id,
        "dag_run_id": run_id,
    }


def persist_drift_history(drift_result: Dict, trigger_result: Dict) -> None:
    resolved_cfg = _resolve_monitoring_config()
    history_path = resolved_cfg["history"]["path"]

    timestamp = drift_result["timestamp"]
    rows_to_write = []
    for row in drift_result["rows"]:
        rows_to_write.append(
            {
                "timestamp": timestamp,
                "feature_name": str(row.get("feature_name", "unknown_feature")),
                "drift_type": str(row.get("drift_type", "unknown")),
                "drift_score": float(row.get("drift_score", 0.0)),
                "drift_detected": bool(row.get("drift_detected", False)),
                "model_metric": row.get("model_metric"),
                "threshold": float(row.get("threshold", 0.0)),
                "ks_statistic": row.get("ks_statistic"),
                "mean_shift": row.get("mean_shift"),
                "std_shift": row.get("std_shift"),
                "overall_drift_score": float(drift_result["drift_score"]),
                "data_drift_score": float(drift_result["data_drift"]["drift_score"]),
                "model_drift_score": float(drift_result["model_drift"]["drift_score"]),
                "triggered": bool(trigger_result.get("triggered", False)),
                "trigger_reason": str(trigger_result.get("reason", "unknown")),
            }
        )

    try:
        frame = pd.DataFrame(rows_to_write)
        table = pa.Table.from_pandas(frame, preserve_index=False)
        write_deltalake(history_path, table, mode="append")
    except Exception as exc:
        logger.warning(
            "Failed to append drift history at %s: %s. Attempting one-time overwrite migration.",
            history_path,
            exc,
        )
        try:
            table = pa.Table.from_pandas(pd.DataFrame(rows_to_write), preserve_index=False)
            write_deltalake(history_path, table, mode="overwrite")
        except Exception as second_exc:
            logger.error("Failed to write drift history at %s: %s", history_path, second_exc)


def run_drift_monitor_job() -> Dict:
    drift_result = evaluate_drift()
    trigger_result = trigger_retraining(drift_result)
    persist_drift_history(drift_result, trigger_result)

    logger.info(
        "Drift monitor cycle finished: drift_detected=%s drift_score=%.4f triggered=%s reason=%s",
        drift_result["drift_detected"],
        drift_result["drift_score"],
        trigger_result.get("triggered", False),
        trigger_result.get("reason", "unknown"),
    )

    return {
        "drift": drift_result,
        "trigger": trigger_result,
    }


if __name__ == "__main__":
    run_drift_monitor_job()
