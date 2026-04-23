import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import requests
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from prometheus_client import Gauge

from utils_global.config_loader import load_config
from utils_global.logger import get_logger
from utils_global.prometheus import build_registry, metric_name, push_registry


logger = get_logger(__name__)


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()


def _population_stability_index(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    expected_values = _safe_numeric(expected).to_numpy()
    actual_values = _safe_numeric(actual).to_numpy()

    if expected_values.size < 2 or actual_values.size < 2:
        return 0.0

    quantiles = np.linspace(0.0, 1.0, bins + 1)
    breakpoints = np.quantile(expected_values, quantiles)
    breakpoints = np.unique(breakpoints)

    if breakpoints.size < 2:
        return 0.0

    expected_counts, _ = np.histogram(expected_values, bins=breakpoints)
    actual_counts, _ = np.histogram(actual_values, bins=breakpoints)

    expected_dist = np.clip(expected_counts / max(expected_counts.sum(), 1), 1e-4, 1.0)
    actual_dist = np.clip(actual_counts / max(actual_counts.sum(), 1), 1e-4, 1.0)

    psi = np.sum((actual_dist - expected_dist) * np.log(actual_dist / expected_dist))
    return float(max(psi, 0.0))


def _resolve_monitoring_config() -> Dict:
    model_config = load_config("configs/model.yaml") or {}
    data_config = load_config("configs/data.yaml") or {}

    monitoring_cfg = model_config.get("monitoring") or {}
    drift_cfg = monitoring_cfg.get("drift") or {}
    retraining_cfg = monitoring_cfg.get("retraining") or {}
    metrics_cfg = monitoring_cfg.get("metrics") or {}
    history_cfg = monitoring_cfg.get("history") or {}

    return {
        "model": model_config,
        "data": data_config,
        "drift": {
            "recent_window": int(drift_cfg.get("recent_window", 5000)),
            "baseline_window": int(drift_cfg.get("baseline_window", 20000)),
            "min_rows": int(drift_cfg.get("min_rows", 200)),
            "psi_bins": int(drift_cfg.get("psi_bins", 10)),
            "data_drift_threshold": float(drift_cfg.get("data_drift_threshold", 0.2)),
            "prediction_drift_threshold": float(drift_cfg.get("prediction_drift_threshold", 0.25)),
            "rmse_increase_threshold": float(drift_cfg.get("rmse_increase_threshold", 0.15)),
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
        "metrics": {
            "job_name": metrics_cfg.get("job_name", "drift_monitor"),
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


def _clip_series(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    lower = s.quantile(0.02)
    upper = s.quantile(0.98)
    return s.clip(lower=lower, upper=upper)
    
def _compute_data_drift(
    baseline_df: pd.DataFrame,
    recent_df: pd.DataFrame,
    features: List[str],
    cfg: Dict,
) -> Dict:
    feature_scores = []
    psi_values = []

    for feature in features:
        if feature not in baseline_df.columns or feature not in recent_df.columns:
            continue

        baseline_series = _clip_series(_safe_numeric(baseline_df[feature]))
        recent_series = _clip_series(_safe_numeric(recent_df[feature]))

        if baseline_series.size < cfg["min_rows"] or recent_series.size < cfg["min_rows"]:
            continue

        baseline_mean = float(baseline_series.mean())
        recent_mean = float(recent_series.mean())
        baseline_std = float(baseline_series.std(ddof=0))
        recent_std = float(recent_series.std(ddof=0))

        mean_shift = abs(recent_mean - baseline_mean) / (abs(baseline_mean) + 1e-6)
        std_shift = abs(recent_std - baseline_std) / (abs(baseline_std) + 1e-6)
        psi = _population_stability_index(
            baseline_series,
            recent_series,
            bins=cfg["psi_bins"],
        )

        score = (0.7 * psi) + (0.15 * mean_shift) + (0.15 * std_shift)

        feature_scores.append(score)
        psi_values.append(psi)

    drift_score = float(np.mean(feature_scores)) if feature_scores else 0.0
    max_psi = float(np.max(psi_values)) if psi_values else 0.0
    drift_detected = bool(
        drift_score >= cfg["data_drift_threshold"] or max_psi >= cfg["data_drift_threshold"]
    )

    return {
        "drift_score": drift_score,
        "max_psi": max_psi,
        "drift_detected": drift_detected,
    }


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


def _compute_prediction_drift(predictions_df: pd.DataFrame, cfg: Dict) -> Dict:
    prepared = _prepare_prediction_frame(predictions_df)

    if prepared.empty or prepared.shape[0] < (cfg["min_rows"] * 2):
        return {
            "drift_score": 0.0,
            "rmse_increase": 0.0,
            "rmse_ratio": 1.0,
            "baseline_rmse": 0.0,
            "recent_rmse": 0.0,
            "drift_detected": False,
        }

    baseline_window = min(cfg["baseline_window"], prepared.shape[0] // 2)
    recent_window = min(cfg["recent_window"], prepared.shape[0] // 2)

    baseline = prepared.iloc[:baseline_window]
    recent = prepared.iloc[-recent_window:]

    baseline_error = baseline["prediction"] - baseline["actual_log_return_lead1"]
    recent_error = recent["prediction"] - recent["actual_log_return_lead1"]

    baseline_rmse = float(np.sqrt(np.mean(np.square(baseline_error))))
    recent_rmse = float(np.sqrt(np.mean(np.square(recent_error))))

    rmse_increase = max((recent_rmse - baseline_rmse) / (baseline_rmse + 1e-6), 0.0)
    rmse_ratio = recent_rmse / (baseline_rmse + 1e-6)

    prediction_psi = _population_stability_index(
        baseline["prediction"],
        recent["prediction"],
        bins=cfg["psi_bins"],
    )

    drift_score = (0.6 * rmse_increase) + (0.4 * prediction_psi)
    drift_detected = bool(
        drift_score >= cfg["prediction_drift_threshold"]
        or rmse_increase >= cfg["rmse_increase_threshold"]
    )

    return {
        "drift_score": float(drift_score),
        "rmse_increase": float(rmse_increase),
        "rmse_ratio": float(rmse_ratio),
        "baseline_rmse": baseline_rmse,
        "recent_rmse": recent_rmse,
        "drift_detected": drift_detected,
    }

def _filter_drift_features(features: List[str]) -> List[str]:
    exclude = {
        "symbol",
        "open_time",
        "date",
        "ingestion_time",
        "is_valid_feature_row",
        "open", "high", "low", "close",
        "hour", "day_of_week",
    }

    return [f for f in features if f not in exclude]
def evaluate_drift() -> Dict:
    resolved_cfg = _resolve_monitoring_config()
    model_cfg = resolved_cfg["model"]
    drift_cfg = resolved_cfg["drift"]

    raw_features = model_cfg.get("features", [])
    features = _filter_drift_features(raw_features)
    
    training_path = model_cfg.get("train_data_path")
    if not training_path or not Path(training_path).exists():
        logger.warning(
            "Training baseline dataset not found at '%s'. Data drift score will be reported as 0.0 until baseline exists.",
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

    data_drift = _compute_data_drift(baseline_df, recent_df, features, drift_cfg)

    predictions_columns = ["open_time", "symbol", "prediction", "log_return"]
    try:
        predictions_df = _load_predictions_frame(columns=predictions_columns)
        predictions_df["open_time"] = pd.to_datetime(predictions_df["open_time"], errors="coerce")
        predictions_df = predictions_df.sort_values("open_time")
    except Exception as exc:
        logger.warning("Failed to load predictions dataset for drift monitoring: %s", exc)
        predictions_df = pd.DataFrame(columns=predictions_columns)

    prediction_drift = _compute_prediction_drift(predictions_df, drift_cfg)

    overall_drift_score = max(data_drift["drift_score"], prediction_drift["drift_score"])
    drift_detected = bool(data_drift["drift_detected"] or prediction_drift["drift_detected"])

    return {
        "drift_score": float(overall_drift_score),
        "drift_detected": drift_detected,
        "data_drift": data_drift,
        "prediction_drift": prediction_drift,
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
            "prediction_drift_score": drift_result["prediction_drift"]["drift_score"],
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


def push_drift_metrics(drift_result: Dict, retraining_triggered: bool) -> None:
    resolved_cfg = _resolve_monitoring_config()
    job_name = resolved_cfg["metrics"]["job_name"]

    registry = build_registry()

    data_metric = Gauge(
        metric_name("data_drift_score"),
        "Current data drift score.",
        registry=registry,
    )
    prediction_metric = Gauge(
        metric_name("prediction_drift_score"),
        "Current prediction drift score.",
        registry=registry,
    )
    alert_metric = Gauge(
        metric_name("drift_alert"),
        "Whether drift alert condition is active (1=true, 0=false).",
        registry=registry,
    )
    rmse_ratio_metric = Gauge(
        metric_name("prediction_rmse_ratio"),
        "Ratio of recent prediction RMSE over baseline RMSE.",
        registry=registry,
    )
    retrain_metric = Gauge(
        metric_name("retraining_triggered"),
        "Whether retraining was triggered on this monitoring cycle (1=true, 0=false).",
        registry=registry,
    )

    data_metric.set(float(drift_result["data_drift"]["drift_score"]))
    prediction_metric.set(float(drift_result["prediction_drift"]["drift_score"]))
    alert_metric.set(1.0 if drift_result.get("drift_detected") else 0.0)
    rmse_ratio_metric.set(float(drift_result["prediction_drift"].get("rmse_ratio", 1.0)))
    retrain_metric.set(1.0 if retraining_triggered else 0.0)

    push_registry(
        registry,
        job_name=job_name,
        grouping_key={"component": "drift_monitor"},
    )


def persist_drift_history(drift_result: Dict, trigger_result: Dict) -> None:
    resolved_cfg = _resolve_monitoring_config()
    history_path = resolved_cfg["history"]["path"]

    row = {
        "event_time": datetime.now(timezone.utc),
        "drift_score": float(drift_result["drift_score"]),
        "drift_detected": bool(drift_result["drift_detected"]),
        "data_drift_score": float(drift_result["data_drift"]["drift_score"]),
        "prediction_drift_score": float(drift_result["prediction_drift"]["drift_score"]),
        "prediction_rmse_ratio": float(drift_result["prediction_drift"].get("rmse_ratio", 1.0)),
        "triggered": bool(trigger_result.get("triggered", False)),
        "trigger_reason": trigger_result.get("reason", "unknown"),
    }

    try:
        table = pa.Table.from_pandas(pd.DataFrame([row]), preserve_index=False)
        write_deltalake(history_path, table, mode="append")
    except Exception as exc:
        logger.warning("Failed to persist drift history at %s: %s", history_path, exc)


def run_drift_monitor_job() -> Dict:
    drift_result = evaluate_drift()
    trigger_result = trigger_retraining(drift_result)

    push_drift_metrics(drift_result, retraining_triggered=trigger_result.get("triggered", False))
    persist_drift_history(drift_result, trigger_result)

    logger.info(
        "Drift monitor cycle finished: drift_detected=%s drift_score=%.4f triggered=%s",
        drift_result["drift_detected"],
        drift_result["drift_score"],
        trigger_result.get("triggered", False),
    )

    return {
        "drift": drift_result,
        "trigger": trigger_result,
    }


if __name__ == "__main__":
    run_drift_monitor_job()
