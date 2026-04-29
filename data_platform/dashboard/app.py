from __future__ import annotations

import pandas as pd
import streamlit as st

from charts import (
    plot_close_price_comparison,
    plot_drift_scores,
    plot_ingestion_rate,
    plot_residuals,
    plot_predicted_vs_actual,
    plot_rolling_rmse,
    plot_row_count_trend,
)
from data_service import compute_latest_updates, load_data
from filters import render_sidebar_filters
from helpers import safe_float
from mlflow_client import get_latest_mlflow_run
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
        ("model_drift_score", "Model drift"),
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
        {"Field": "Model metric", "Value": _format_display_value(latest_row.get("model_metric"))},
    ]
    st.dataframe(pd.DataFrame(detail_rows), width="content", hide_index=True)


def render_platform_snapshot(
    model_config,
    mlflow_config,
    mlflow_run,
    gold_frame: pd.DataFrame,
    drift_frame: pd.DataFrame,
) -> None:
    st.subheader("Platform Snapshot")

    monitoring_cfg = model_config.get("monitoring", {}) or {}
    drift_cfg = monitoring_cfg.get("drift", {}) or {}
    retraining_cfg = monitoring_cfg.get("retraining", {}) or {}
    scheduler_cfg = monitoring_cfg.get("scheduler", {}) or {}

    feature_count = len([feature for feature in model_config.get("features_long", model_config.get("features_short", model_config.get("features", []))) if feature != "symbol"])
    max_threshold = max(
        float(drift_cfg.get("data_drift_threshold", 0.2)),
        float(drift_cfg.get("model_drift_threshold", drift_cfg.get("prediction_drift_threshold", 0.25))),
    )

    metric_row_1 = st.columns(2)
    metric_row_1[0].metric("Target", str(model_config.get("target", "log_return_lead1")))
    metric_row_1[1].metric("Features", str(feature_count))

    metric_row_2 = st.columns(2)
    metric_row_2[0].metric("Drift threshold", f"{max_threshold:.2f}")
    metric_row_2[1].metric("Cooldown", f"{int(retraining_cfg.get('cooldown_minutes', 30))}m")

    st.caption(
        "Model registry: "
        f"{mlflow_config.get('model_name', model_config.get('model_name', 'crypto-return-predictor'))} | "
        f"Tracking URI: {mlflow_config.get('tracking_uri', 'N/A')} | "
        f"Monitoring every {int(scheduler_cfg.get('interval_minutes', 5))} minute(s)"
    )

    run_id = mlflow_run.get("run_id") if isinstance(mlflow_run, dict) else None
    started_at = mlflow_run.get("started_at") if isinstance(mlflow_run, dict) else None
    run_metrics = mlflow_run.get("metrics", {}) if isinstance(mlflow_run, dict) else {}
    run_rmse = safe_float(run_metrics.get("rmse")) if isinstance(run_metrics, dict) else None

    st.caption(
        "Latest MLflow run: "
        f"{str(run_id)[:12] if run_id else 'N/A'} | "
        f"Started: {_format_display_value(started_at) if started_at else 'N/A'} | "
        f"RMSE: {f'{run_rmse:.6f}' if run_rmse is not None else 'N/A'}"
    )

    render_market_snapshot(gold_frame)
    st.divider()
    render_drift_posture(drift_frame)


def render_overview(gold_frame, predictions_frame, drift_frame, mlflow_run) -> None:
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

    mlflow_metrics = mlflow_run.get("metrics", {}) if isinstance(mlflow_run, dict) else {}
    latest_rmse = safe_float(mlflow_metrics.get("rmse")) if isinstance(mlflow_metrics, dict) else None
    latest_directional = (
        safe_float(mlflow_metrics.get("directional_accuracy")) if isinstance(mlflow_metrics, dict) else None
    )

    metric_cols = st.columns(5)
    metric_cols[0].metric("Latest price", f"{latest_price:.4f}" if latest_price is not None else "N/A")
    metric_cols[1].metric("Latest prediction", f"{latest_prediction:.6f}" if latest_prediction is not None else "N/A")
    metric_cols[2].metric("Drift score", f"{latest_drift:.4f}" if latest_drift is not None else "N/A")
    metric_cols[3].metric("Latest RMSE", f"{latest_rmse:.6f}" if latest_rmse is not None else "N/A")
    metric_cols[4].metric(
        "Directional accuracy",
        f"{latest_directional * 100:.2f}%" if latest_directional is not None else "N/A",
    )


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
    
    st.plotly_chart(plot_predicted_vs_actual(predictions_frame), width="stretch", theme=None)

    st.plotly_chart(plot_close_price_comparison(predictions_frame), width="stretch", theme=None)

    error_col_1, error_col_2 = st.columns(2)
    with error_col_1:
        st.plotly_chart(plot_residuals(predictions_frame), width="stretch", theme=None)
    with error_col_2:
        st.plotly_chart(plot_rolling_rmse(predictions_frame), width="stretch", theme=None)


def render_drift_monitoring(model_config, drift_frame, drift_feature_frame) -> None:
    st.subheader("Drift Monitoring")

    if drift_frame.empty:
        st.info("No drift history rows found in the selected window.")
        return

    drift_cfg = model_config.get("monitoring", {}).get("drift", {}) or {}

    st.caption(
        f"Alert thresholds: data {float(drift_cfg.get('data_drift_threshold', 0.2)):.2f}, "
        f"model {float(drift_cfg.get('model_drift_threshold', drift_cfg.get('prediction_drift_threshold', 0.25))):.2f}. "
        "Automatic retraining is guarded by cooldown logic in models.monitoring.drift."
    )

    st.plotly_chart(
        plot_drift_scores(
            drift_frame,
            data_threshold=float(drift_cfg.get("data_drift_threshold", 0.2)),
            model_threshold=float(
                drift_cfg.get("model_drift_threshold", drift_cfg.get("prediction_drift_threshold", 0.25))
            ),
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
            "model_drift_score",
            "model_metric",
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

    if not drift_feature_frame.empty:
        feature_columns = [
            column
            for column in ["event_time", "feature_name", "drift_score", "drift_detected"]
            if column in drift_feature_frame.columns
        ]

        if feature_columns:
            sort_columns = [
                column
                for column in ["event_time", "drift_score"]
                if column in drift_feature_frame.columns
            ]
            top_feature_rows = drift_feature_frame.copy()
            if sort_columns:
                ascending = [True if column == "event_time" else False for column in sort_columns]
                top_feature_rows = top_feature_rows.sort_values(sort_columns, ascending=ascending)

            if "drift_score" in top_feature_rows.columns:
                top_feature_rows = top_feature_rows.tail(20).sort_values("drift_score", ascending=False).head(10)
            else:
                top_feature_rows = top_feature_rows.tail(10)

            st.caption("Top drifting features from recent drift monitor cycles")
            st.dataframe(top_feature_rows[feature_columns], width="stretch", hide_index=True)


def render_recent_predictions(predictions_frame: pd.DataFrame) -> None:
    st.subheader("Recent Predictions")

    if predictions_frame.empty:
        st.info("No prediction rows are available for the selected time window.")
        return

    sort_column = "open_time" if "open_time" in predictions_frame.columns else predictions_frame.columns[0]

    preferred_columns = [
        "open_time",
        "model_version",
        "prediction",
        "actual_log_return_lead1",
        "residual",
        "predicted_close",
        "actual_close",
    ]
    display_columns = [column for column in preferred_columns if column in predictions_frame.columns]

    if not display_columns:
        st.info("Prediction rows are missing the expected display columns.")
        return

    recent_predictions = (
        predictions_frame.sort_values(sort_column)
        .tail(10)
        .iloc[::-1]
        .loc[:, display_columns]
        .copy()
    )

    for column in recent_predictions.columns:
        recent_predictions[column] = recent_predictions[column].map(_format_display_value)

    st.caption("Latest 10 predictions for the selected symbol and model version.")
    st.dataframe(recent_predictions, width="stretch", hide_index=True)


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
    drift_feature_frame = data.get("drift_features", pd.DataFrame())
    mlflow_run = get_latest_mlflow_run(mlflow_config, refresh_nonce=refresh_nonce)

    st.divider()
    render_platform_snapshot(
        model_config=model_config,
        mlflow_config=mlflow_config,
        mlflow_run=mlflow_run,
        gold_frame=gold_frame,
        drift_frame=drift_frame,
    )

    st.divider()
    render_overview(
        gold_frame=gold_frame,
        predictions_frame=predictions_frame,
        drift_frame=drift_frame,
        mlflow_run=mlflow_run,
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
    render_drift_monitoring(
        model_config=model_config,
        drift_frame=drift_frame,
        drift_feature_frame=drift_feature_frame,
    )

    st.divider()
    render_recent_predictions(predictions_frame=predictions_frame)


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
