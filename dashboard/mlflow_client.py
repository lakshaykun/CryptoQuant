from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import requests
import streamlit as st


def _tracking_uri(mlflow_config: Dict[str, Any]) -> str:
    uri = mlflow_config.get("tracking_uri") or mlflow_config.get("local_tracking_uri") or ""
    return str(uri).rstrip("/")


@st.cache_data(show_spinner=False, ttl=30)
def get_latest_mlflow_run(mlflow_config: Dict[str, Any], refresh_nonce: int = 0) -> Dict[str, Any]:
    _ = refresh_nonce

    base_url = _tracking_uri(mlflow_config)
    experiment_name = str(mlflow_config.get("experiment_name", "")).strip()

    if not base_url or not experiment_name:
        return {}

    try:
        exp_response = requests.get(
            f"{base_url}/api/2.0/mlflow/experiments/get-by-name",
            params={"experiment_name": experiment_name},
            timeout=5,
        )
        exp_response.raise_for_status()
        exp_payload = exp_response.json() or {}
        experiment = exp_payload.get("experiment") or {}
        experiment_id = experiment.get("experiment_id")
        if not experiment_id:
            return {}

        search_response = requests.post(
            f"{base_url}/api/2.0/mlflow/runs/search",
            json={
                "experiment_ids": [str(experiment_id)],
                "max_results": "1",
                "order_by": ["attribute.start_time DESC"],
            },
            timeout=5,
        )
        search_response.raise_for_status()
        runs = (search_response.json() or {}).get("runs") or []
        if not runs:
            return {}

        run = runs[0]
        info = run.get("info") or {}
        data = run.get("data") or {}

        metrics = {}
        for metric in data.get("metrics") or []:
            key = metric.get("key")
            value = metric.get("value")
            if key is None:
                continue
            try:
                metrics[str(key)] = float(value)
            except (TypeError, ValueError):
                continue

        params = {
            str(param.get("key")): str(param.get("value"))
            for param in (data.get("params") or [])
            if param.get("key") is not None
        }

        tags = {
            str(tag.get("key")): str(tag.get("value"))
            for tag in (data.get("tags") or [])
            if tag.get("key") is not None
        }

        started_at = None
        start_time = info.get("start_time")
        if start_time is not None:
            try:
                started_at = datetime.fromtimestamp(int(start_time) / 1000, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                started_at = None

        return {
            "run_id": info.get("run_id"),
            "status": info.get("status"),
            "started_at": started_at,
            "metrics": metrics,
            "params": params,
            "tags": tags,
        }
    except Exception:
        return {}
