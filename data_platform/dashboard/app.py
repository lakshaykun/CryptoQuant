from __future__ import annotations

import html

import pandas as pd
import streamlit as st

from charts import (
    plot_close_price_comparison,
    plot_drift_scores,
    plot_ingestion_rate,
    plot_predicted_vs_actual,
    plot_prometheus_series,
    plot_residuals,
    plot_rolling_rmse,
    plot_row_count_trend,
)
from data_service import compute_latest_updates, load_data
from filters import render_sidebar_filters
from helpers import safe_float
from prometheus_client import query_prometheus, query_prometheus_range
from settings import (
    AUTO_REFRESH_SECONDS,
    DATA_CONFIG_PATH,
    MODEL_CONFIG_PATH,
    MLFLOW_CONFIG_PATH,
    REPO_ROOT,
    apply_app_styles,
    apply_page_config,
    load_yaml,
)


def _format_display_value(value) -> str:
    try:
        if pd.isna(value):
            return "N/A"
    except Exception:
        pass

    if hasattr(value, "strftime"):
        try:
            return value.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass

    numeric = safe_float(value)
    if numeric is not None:
        magnitude = abs(numeric)
        if magnitude >= 1000:
            return f"{numeric:,.2f}"
        if magnitude >= 1:
            return f"{numeric:.4f}"
        return f"{numeric:.6f}"

    return str(value)


def _latest_row(frame: pd.DataFrame):
    if frame.empty:
        return None

    sort_columns = [column for column in ["event_time", "open_time", "ingestion_time"] if column in frame.columns]
    if sort_columns:
        return frame.sort_values(sort_columns).iloc[-1]

    return frame.iloc[-1]


def render_hero_banner(filters, model_config, mlflow_config) -> None:
    model_name = model_config.get("model_name", "crypto-return-predictor")
    target_name = model_config.get("target", "log_return_lead1")
    registry_model_name = mlflow_config.get("model_name", model_name)
    tracking_uri = mlflow_config.get("tracking_uri", "N/A")

    window_label = (
        f"{filters['start'].strftime('%Y-%m-%d %H:%M')} → {filters['end'].strftime('%Y-%m-%d %H:%M')} UTC"
    )

    st.markdown(
        f"""
        <div class="hero-card">
            <div class="hero-eyebrow">CryptoQuant MLOps Console</div>
            <div class="hero-title">Dark, glassy observability for the {html.escape(filters['symbol'])} pipeline</div>
            <p class="hero-copy">
                Gold-table visibility, model quality, Prometheus health, and automatic retraining signals in one control surface.
                MLflow registry: {html.escape(registry_model_name)}. Tracking URI: {html.escape(tracking_uri)}.
            </p>
            <div class="hero-meta">
                <span class="hero-pill">Symbol: {html.escape(filters['symbol'])}</span>
                        <span class="hero-pill">Model: {html.escape(model_name)}</span>
                <span class="hero-pill">Window: {html.escape(window_label)}</span>
                <span class="hero-pill">Target: {html.escape(target_name)}</span>
                <span class="hero-pill">Registry: {html.escape(registry_model_name)}</span>
                <span class="hero-pill">Auto-refresh: {AUTO_REFRESH_SECONDS}s</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
        )


def render_market_snapshot(gold_frame: pd.DataFrame) -> None:
    st.markdown("##### Latest Market Snapshot")

    if gold_frame.empty:
        st.info("No Gold rows found for the selected window.")
        return

    latest_row = _latest_row(gold_frame)
    if latest_row is None:
        st.info("No Gold rows found for the selected window.")
        return

    time_label = None
    for candidate in ["open_time", "ingestion_time"]:
        if candidate in latest_row.index:
            time_label = latest_row.get(candidate)
            if time_label is not None:
                break

    if time_label is not None:
        st.caption(f"Latest observation at {_format_display_value(time_label)}")

    metric_specs = [
        ("close", "Latest close"),
        ("log_return", "Latest log return"),
        ("volume", "Volume"),
        ("volatility_ratio", "Volatility ratio"),
    ]
    metric_cols = st.columns(len(metric_specs))
    for metric_col, (field_name, label) in zip(metric_cols, metric_specs):
        metric_col.metric(label, _format_display_value(latest_row.get(field_name)) if field_name in latest_row.index else "N/A")

    detail_specs = [
        ("trend_strength", "Trend strength"),
        ("buy_ratio", "Buy ratio"),
        ("momentum", "Momentum"),
        ("volume_spike", "Volume spike"),
        ("ma_5", "MA 5"),
        ("ma_20", "MA 20"),
        ("volatility", "Volatility"),
        ("price_range_ratio", "Price range ratio"),
    ]
    detail_rows = [
        {"Feature": label, "Value": _format_display_value(latest_row.get(field_name))}
        for field_name, label in detail_specs
        if field_name in latest_row.index
    ]

    if detail_rows:
        st.dataframe(pd.DataFrame(detail_rows), width="content", hide_index=True)


def render_drift_posture(drift_frame: pd.DataFrame) -> None:
    st.markdown("##### Latest Drift Posture")

    if drift_frame.empty:
        st.info("No drift history rows found in the selected window.")
        return

    latest_row = _latest_row(drift_frame)
    if latest_row is None:
        st.info("No drift history rows found in the selected window.")
        return

    trigger_reason = _format_display_value(latest_row.get("trigger_reason")) if "trigger_reason" in latest_row.index else "N/A"
    if bool(latest_row.get("triggered", False)):
        retraining_state = "Triggered"
    elif trigger_reason == "cooldown_active":
        retraining_state = "Cooling down"
    else:
        retraining_state = "Idle"

    metric_specs = [
        ("drift_score", "Overall drift"),
        ("data_drift_score", "Data drift"),
        ("prediction_drift_score", "Prediction drift"),
        (None, "Retraining"),
    ]
    metric_cols = st.columns(len(metric_specs))
    for metric_col, (field_name, label) in zip(metric_cols, metric_specs):
        if field_name is None:
            metric_col.metric(label, retraining_state)
        else:
            metric_col.metric(label, _format_display_value(latest_row.get(field_name)) if field_name in latest_row.index else "N/A")

    detail_rows = [
        {"Field": "Event time", "Value": _format_display_value(latest_row.get("event_time"))},
        {"Field": "Drift detected", "Value": "Yes" if bool(latest_row.get("drift_detected", False)) else "No"},
        {"Field": "Trigger reason", "Value": trigger_reason},
        {"Field": "RMSE ratio", "Value": _format_display_value(latest_row.get("prediction_rmse_ratio"))},
    ]
    st.dataframe(pd.DataFrame(detail_rows), width="content", hide_index=True)


def render_platform_snapshot(model_config, mlflow_config, gold_frame: pd.DataFrame, drift_frame: pd.DataFrame) -> None:
    st.subheader("Platform Snapshot")

    monitoring_cfg = model_config.get("monitoring", {}) or {}
    drift_cfg = monitoring_cfg.get("drift", {}) or {}
    retraining_cfg = monitoring_cfg.get("retraining", {}) or {}
    scheduler_cfg = monitoring_cfg.get("scheduler", {}) or {}

    feature_count = len([feature for feature in model_config.get("features", []) if feature != "symbol"])
    max_threshold = max(
        float(drift_cfg.get("data_drift_threshold", 0.2)),
        float(drift_cfg.get("prediction_drift_threshold", 0.25)),
    )

    metric_cols = st.columns(4)
    metric_cols[0].metric("Target", str(model_config.get("target", "log_return_lead1")))
    metric_cols[1].metric("Features", str(feature_count))
    metric_cols[2].metric("Drift threshold", f"{max_threshold:.2f}")
    metric_cols[3].metric("Cooldown", f"{int(retraining_cfg.get('cooldown_minutes', 30))}m")

    st.caption(
        "Model registry: "
        f"{mlflow_config.get('model_name', model_config.get('model_name', 'crypto-return-predictor'))} | "
        f"Tracking URI: {mlflow_config.get('tracking_uri', 'N/A')} | "
        f"Monitoring every {int(scheduler_cfg.get('interval_minutes', 5))} minute(s)"
    )

    snapshot_col_1, snapshot_col_2 = st.columns([1.1, 1])
    with snapshot_col_1:
        render_market_snapshot(gold_frame)

    with snapshot_col_2:
        render_drift_posture(drift_frame)


def render_service_health(prometheus_url: str, refresh_nonce: int) -> None:
    st.subheader("Operational Health")
    st.caption("Core services and monitoring side effects reported via Prometheus.")

    service_specs = [
        ("API", 'up{job="fastapi"}'),
        ("Prometheus", 'up{job="prometheus"}'),
        ("Spark streaming", 'up{job="spark-streaming"}'),
        ("Spark predictions", 'up{job="spark-predictions"}'),
        ("Airflow statsd", 'up{job="airflow-statsd"}'),
        ("Kafka exporter", 'up{job="kafka"}'),
        ("Node exporter", 'up{job="node"}'),
        ("cAdvisor", 'up{job="cadvisor"}'),
    ]

    for row_specs in [service_specs[:4], service_specs[4:]]:
        service_cols = st.columns(len(row_specs))
        for service_col, (label, query) in zip(service_cols, row_specs):
            value = query_prometheus(base_url=prometheus_url, query=query, refresh_nonce=refresh_nonce)
            service_col.metric(label, "Up" if value == 1 else ("Down" if value == 0 else "N/A"))

    monitoring_cols = st.columns(4)

    drift_alert = query_prometheus(
        base_url=prometheus_url,
        query="max(cryptoquant_drift_alert)",
        refresh_nonce=refresh_nonce,
    )
    retraining_triggered = query_prometheus(
        base_url=prometheus_url,
        query="max(cryptoquant_retraining_triggered)",
        refresh_nonce=refresh_nonce,
    )
    training_success = query_prometheus(
        base_url=prometheus_url,
        query='max(cryptoquant_airflow_dag_last_run_status{dag_id="model_training_pipeline",status="success"})',
        refresh_nonce=refresh_nonce,
    )
    training_failure = query_prometheus(
        base_url=prometheus_url,
        query='max(cryptoquant_airflow_dag_last_run_status{dag_id="model_training_pipeline",status="failure"})',
        refresh_nonce=refresh_nonce,
    )
    training_duration = query_prometheus(
        base_url=prometheus_url,
        query='max(cryptoquant_airflow_dag_duration_seconds{dag_id="model_training_pipeline"})',
        refresh_nonce=refresh_nonce,
    )

    monitoring_cols[0].metric("Drift alert", "Active" if drift_alert == 1 else ("Idle" if drift_alert == 0 else "N/A"))
    monitoring_cols[1].metric(
        "Retraining",
        "Triggered" if retraining_triggered == 1 else ("Idle" if retraining_triggered == 0 else "N/A"),
    )
    monitoring_cols[2].metric(
        "Training DAG",
        "Success" if training_success == 1 else ("Failure" if training_failure == 1 else "N/A"),
    )
    monitoring_cols[3].metric(
        "Latest duration",
        f"{training_duration:.1f}s" if training_duration is not None else "N/A",
    )


def render_overview(gold_frame, predictions_frame, drift_frame, prometheus_url: str, refresh_nonce: int) -> None:
    st.subheader("Operational Overview")

    latest_price = None
    if not gold_frame.empty and "close" in gold_frame.columns and gold_frame["close"].notna().any():
        latest_price = safe_float(gold_frame["close"].dropna().iloc[-1])

    latest_prediction = None
    if (
        not predictions_frame.empty
        and "prediction" in predictions_frame.columns
        and predictions_frame["prediction"].notna().any()
    ):
        latest_prediction = safe_float(predictions_frame["prediction"].dropna().iloc[-1])

    latest_drift = None
    if not drift_frame.empty and "drift_score" in drift_frame.columns and drift_frame["drift_score"].notna().any():
        latest_drift = safe_float(drift_frame["drift_score"].dropna().iloc[-1])

    pipeline_latency = query_prometheus(
        base_url=prometheus_url,
        query="max(cryptoquant_streaming_lag_seconds)",
        refresh_nonce=refresh_nonce,
    )

    metric_cols = st.columns(4)
    metric_cols[0].metric("Latest price", f"{latest_price:.4f}" if latest_price is not None else "N/A")
    metric_cols[1].metric("Latest prediction", f"{latest_prediction:.6f}" if latest_prediction is not None else "N/A")
    metric_cols[2].metric("Drift score", f"{latest_drift:.4f}" if latest_drift is not None else "N/A")
    metric_cols[3].metric("Pipeline latency", f"{pipeline_latency:.1f}s" if pipeline_latency is not None else "N/A")


def render_gold_pipeline(data_config, gold_frame, symbol, start, end, refresh_nonce: int) -> None:
    st.subheader("Gold Pipeline")

    if gold_frame.empty:
        st.info("No Gold rows found for the selected symbol and time range.")
        return

    time_col = "open_time" if "open_time" in gold_frame.columns else "ingestion_time"
    chart_col_1, chart_col_2 = st.columns([2, 1])

    with chart_col_1:
        st.plotly_chart(plot_ingestion_rate(gold_frame, time_col=time_col), width="stretch", theme=None)

    with chart_col_2:
        st.plotly_chart(plot_row_count_trend(gold_frame, time_col=time_col), width="stretch", theme=None)

    updates = compute_latest_updates(
        data_config=data_config,
        symbol=symbol,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=str(REPO_ROOT),
    )
    st.dataframe(updates, width="stretch", hide_index=True)


def render_model_behavior(predictions_frame) -> None:
    st.subheader("Model Behavior")

    if predictions_frame.empty:
        st.info("No prediction rows with actual comparisons are available for this time window.")
        return

    comparison_col_1, comparison_col_2 = st.columns(2)
    with comparison_col_1:
        st.plotly_chart(plot_predicted_vs_actual(predictions_frame), width="stretch", theme=None)
    with comparison_col_2:
        st.plotly_chart(plot_close_price_comparison(predictions_frame), width="stretch", theme=None)

    error_col_1, error_col_2 = st.columns(2)
    with error_col_1:
        st.plotly_chart(plot_residuals(predictions_frame), width="stretch", theme=None)
    with error_col_2:
        st.plotly_chart(plot_rolling_rmse(predictions_frame), width="stretch", theme=None)


def render_drift_monitoring(model_config, drift_frame) -> None:
    st.subheader("Drift Monitoring")

    if drift_frame.empty:
        st.info("No drift history rows found in the selected window.")
        return

    drift_cfg = model_config.get("monitoring", {}).get("drift", {}) or {}

    st.caption(
        f"Alert thresholds: data {float(drift_cfg.get('data_drift_threshold', 0.2)):.2f}, "
        f"prediction {float(drift_cfg.get('prediction_drift_threshold', 0.25)):.2f}. "
        "Automatic retraining is guarded by cooldown logic in models.monitoring.drift."
    )

    st.plotly_chart(
        plot_drift_scores(
            drift_frame,
            data_threshold=float(drift_cfg.get("data_drift_threshold", 0.2)),
            prediction_threshold=float(drift_cfg.get("prediction_drift_threshold", 0.25)),
        ),
        width="stretch",
        theme=None,
    )

    recent_columns = [
        column
        for column in [
            "event_time",
            "drift_score",
            "data_drift_score",
            "prediction_drift_score",
            "prediction_rmse_ratio",
            "drift_detected",
            "triggered",
            "trigger_reason",
        ]
        if column in drift_frame.columns
    ]
    if recent_columns:
        sort_column = "event_time" if "event_time" in drift_frame.columns else recent_columns[0]
        recent_history = drift_frame.sort_values(sort_column).tail(8)[recent_columns]
        st.dataframe(recent_history, width="stretch", hide_index=True)


def render_api_system_metrics(prometheus_url: str, start, end, refresh_nonce: int) -> None:
    st.subheader("API and Serving Telemetry")

    req_rate_query = 'sum(rate(cryptoquant_api_requests_total{path="/predict"}[1m]))'
    p95_query = "histogram_quantile(0.95, sum(rate(cryptoquant_api_request_latency_seconds_bucket[5m])) by (le))"
    err_rate_query = (
        'sum(rate(cryptoquant_api_requests_total{status=~"4..|5.."}[5m])) '
        '/ clamp_min(sum(rate(cryptoquant_api_requests_total[5m])), 1e-9)'
    )
    prometheus_health_query = 'up{job="prometheus"}'

    current_request_rate = query_prometheus(
        base_url=prometheus_url,
        query=req_rate_query,
        refresh_nonce=refresh_nonce,
    )
    current_latency_p95 = query_prometheus(
        base_url=prometheus_url,
        query=p95_query,
        refresh_nonce=refresh_nonce,
    )
    current_error_rate = query_prometheus(
        base_url=prometheus_url,
        query=err_rate_query,
        refresh_nonce=refresh_nonce,
    )
    prometheus_health = query_prometheus(
        base_url=prometheus_url,
        query=prometheus_health_query,
        refresh_nonce=refresh_nonce,
    )

    card_cols = st.columns(4)
    card_cols[0].metric("Prometheus health", "Up" if prometheus_health == 1 else ("Down" if prometheus_health == 0 else "N/A"))
    card_cols[1].metric("Request rate", f"{current_request_rate:.2f}/s" if current_request_rate is not None else "N/A")
    card_cols[2].metric("p95 latency", f"{current_latency_p95:.3f}s" if current_latency_p95 is not None else "N/A")
    card_cols[3].metric("Error rate", f"{100.0 * current_error_rate:.2f}%" if current_error_rate is not None else "N/A")

    req_rate = query_prometheus_range(
        base_url=prometheus_url,
        query=req_rate_query,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
    )
    p95_latency = query_prometheus_range(
        base_url=prometheus_url,
        query=p95_query,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
    )
    error_rate = query_prometheus_range(
        base_url=prometheus_url,
        query=err_rate_query,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
    )

    if req_rate.empty and p95_latency.empty and error_rate.empty:
        st.warning("Prometheus metrics are unavailable. Check the Prometheus URL and service status.")
        return

    prom_col_1, prom_col_2, prom_col_3 = st.columns(3)

    with prom_col_1:
        if req_rate.empty:
            st.info("No request-rate data in range.")
        else:
            st.plotly_chart(plot_prometheus_series(req_rate, "Request rate", "Req/s"), width="stretch", theme=None)

    with prom_col_2:
        if p95_latency.empty:
            st.info("No latency data in range.")
        else:
            st.plotly_chart(plot_prometheus_series(p95_latency, "p95 latency", "Seconds"), width="stretch", theme=None)

    with prom_col_3:
        if error_rate.empty:
            st.info("No error-rate data in range.")
        else:
            st.plotly_chart(plot_prometheus_series(error_rate, "Error rate", "Ratio"), width="stretch", theme=None)


def render_dashboard_body(data_config, model_config, mlflow_config, filters, refresh_nonce: int) -> None:
    data = load_data(
        data_config=data_config,
        model_config=model_config,
        symbol=filters["symbol"],
        start=filters["start"],
        end=filters["end"],
        model_version=filters["model_version"],
        refresh_nonce=refresh_nonce,
        repo_root=str(REPO_ROOT),
    )

    gold_frame = data["gold"]
    predictions_frame = data["predictions"]
    drift_frame = data["drift"]

    render_hero_banner(filters=filters, model_config=model_config, mlflow_config=mlflow_config)

    st.divider()
    render_platform_snapshot(
        model_config=model_config,
        mlflow_config=mlflow_config,
        gold_frame=gold_frame,
        drift_frame=drift_frame,
    )

    st.divider()
    render_overview(
        gold_frame=gold_frame,
        predictions_frame=predictions_frame,
        drift_frame=drift_frame,
        prometheus_url=filters["prometheus_url"],
        refresh_nonce=refresh_nonce,
    )

    st.divider()
    render_gold_pipeline(
        data_config=data_config,
        gold_frame=gold_frame,
        symbol=filters["symbol"],
        start=filters["start"],
        end=filters["end"],
        refresh_nonce=refresh_nonce,
    )

    st.divider()
    render_model_behavior(predictions_frame=predictions_frame)

    st.divider()
    render_drift_monitoring(model_config=model_config, drift_frame=drift_frame)

    st.divider()
    render_service_health(prometheus_url=filters["prometheus_url"], refresh_nonce=refresh_nonce)

    st.divider()
    render_api_system_metrics(
        prometheus_url=filters["prometheus_url"],
        start=filters["start"],
        end=filters["end"],
        refresh_nonce=refresh_nonce,
    )


def main() -> None:
    apply_page_config()

    apply_app_styles()

    data_config = load_yaml(DATA_CONFIG_PATH)
    model_config = load_yaml(MODEL_CONFIG_PATH)
    mlflow_config = load_yaml(MLFLOW_CONFIG_PATH)

    if "refresh_nonce" not in st.session_state:
        st.session_state["refresh_nonce"] = 0

    filters = render_sidebar_filters(
        data_config=data_config,
        refresh_nonce=st.session_state["refresh_nonce"],
        repo_root=str(REPO_ROOT),
    )

    if filters["refresh_clicked"]:
        st.session_state["refresh_nonce"] += 1
        st.rerun()

    def render_fragment_body() -> None:
        render_dashboard_body(
            data_config=data_config,
            model_config=model_config,
            mlflow_config=mlflow_config,
            filters=filters,
            refresh_nonce=st.session_state["refresh_nonce"],
        )

    if hasattr(st, "fragment"):
        @st.fragment(run_every=f"{AUTO_REFRESH_SECONDS}s")
        def render_fragment() -> None:
            render_fragment_body()

        render_fragment()
    else:
        st.warning("Auto-refresh requires a newer Streamlit version. Use manual refresh in the sidebar.")
        render_fragment_body()


if __name__ == "__main__":
    main()
