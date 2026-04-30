"""
app.py – CryptoQuant Model Observatory
5-tab production observability dashboard. No auto-refresh; manual Refresh button only.
Tab rendering is lazy — only the active tab loads data.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st

import charts
from data_service import (
    compute_latest_updates,
    compute_regime_performance,
    get_drift_history,
    get_feature_snapshot,
    get_predictions,
    get_training_baseline_stats,
    simulate_pnl,
)
from filters import render_sidebar_filters
from helpers import safe_float
from mlflow_client import get_latest_mlflow_run
from settings import (
    DATA_CONFIG_PATH,
    MODEL_CONFIG_PATH,
    MLFLOW_CONFIG_PATH,
    REPO_ROOT,
    apply_app_styles,
    apply_page_config,
    load_yaml,
)


# ---------------------------------------------------------------------------
# Metric helpers – no sklearn dependency
# ---------------------------------------------------------------------------

def _rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def _mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y_true - y_pred)))


def _r2(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    return float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0


def _f1_macro(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    classes = np.unique(np.concatenate([y_true, y_pred]))
    f1s = []
    for c in classes:
        tp = np.sum((y_pred == c) & (y_true == c))
        fp = np.sum((y_pred == c) & (y_true != c))
        fn = np.sum((y_pred != c) & (y_true == c))
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1s.append(2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0)
    return float(np.mean(f1s)) if f1s else 0.0


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def _fmt(v) -> str:
    try:
        if pd.isna(v):
            return "N/A"
    except Exception:
        pass
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d %H:%M UTC")
    f = safe_float(v)
    if f is not None:
        mag = abs(f)
        if mag >= 1000:
            return f"{f:,.2f}"
        if mag >= 1:
            return f"{f:.4f}"
        return f"{f:.6f}"
    return str(v)


def _metrics(specs: list) -> None:
    """Render a horizontal row of st.metric cards. specs = [(label, value), ...]"""
    cols = st.columns(len(specs))
    for col, (label, value) in zip(cols, specs):
        col.metric(label, _fmt(value))


def _empty(label: str) -> None:
    st.info(f"No data — {label}")


_chart_counter: list[int] = [0]  # mutable sentinel for auto-key


def _chart(fig, **kw) -> None:
    _chart_counter[0] += 1
    st.plotly_chart(fig, use_container_width=True, theme=None, key=f"chart_{_chart_counter[0]}", **kw)


# ---------------------------------------------------------------------------
# Tab 1 — Overview
# ---------------------------------------------------------------------------

def _tab_overview(data_config, model_config, mlflow_config, filters, nonce):
    gold = get_feature_snapshot(
        data_config=data_config, model_config=model_config,
        symbol=filters["symbol"], start=filters["start"], end=filters["end"],
        refresh_nonce=nonce, repo_root=str(REPO_ROOT),
    )
    preds = get_predictions(
        data_config=data_config, model_config=model_config,
        symbol=filters["symbol"], start=filters["start"], end=filters["end"],
        model_version=filters["model_version"], refresh_nonce=nonce, repo_root=str(REPO_ROOT),
    )
    drift, _ = get_drift_history(
        model_config=model_config, start=filters["start"], end=filters["end"],
        refresh_nonce=nonce, repo_root=str(REPO_ROOT),
    )
    mlflow_run = get_latest_mlflow_run(mlflow_config, refresh_nonce=nonce)
    mlflow_metrics = mlflow_run.get("metrics", {}) if isinstance(mlflow_run, dict) else {}

    latest_pred_time = preds["open_time"].max() if not preds.empty and "open_time" in preds.columns else None
    valid_pct = (
        gold["is_valid_feature_row"].mean() * 100
        if not gold.empty and "is_valid_feature_row" in gold.columns else None
    )
    latest_drift = (
        drift["drift_score"].dropna().iloc[-1]
        if not drift.empty and "drift_score" in drift.columns else None
    )

    st.subheader("System Health")
    _metrics([
        ("Latest prediction", latest_pred_time),
        ("Predictions in window", len(preds)),
        ("Valid feature rows", f"{valid_pct:.1f}%" if valid_pct is not None else "N/A"),
        ("Drift score", latest_drift),
        ("Last retrain", mlflow_run.get("started_at") if isinstance(mlflow_run, dict) else None),
        ("MLflow RMSE", safe_float(mlflow_metrics.get("rmse"))),
    ])

    st.divider()
    if gold.empty:
        _empty("gold data")
        return

    c1, c2 = st.columns([2, 1])
    with c1:
        st.caption("Predictions per minute")
        if not preds.empty:
            _chart(charts.plot_ingestion_rate(preds, time_col="open_time"))
        else:
            _empty("predictions")
    with c2:
        st.caption("Symbol distribution")
        if "symbol" in gold.columns:
            _chart(charts.plot_symbol_distribution(gold))

    st.caption("Data latency  (ingestion_time − open_time)")
    _chart(charts.plot_data_latency(gold))

    st.divider()
    st.caption("Gold pipeline freshness")
    updates = compute_latest_updates(
        data_config=data_config, symbol=filters["symbol"],
        start=filters["start"], end=filters["end"],
        refresh_nonce=nonce, repo_root=str(REPO_ROOT),
    )
    st.dataframe(updates, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Tab 2 — Model Performance
# ---------------------------------------------------------------------------

def _tab_model_performance(data_config, model_config, filters, nonce):
    preds = get_predictions(
        data_config=data_config, model_config=model_config,
        symbol=filters["symbol"], start=filters["start"], end=filters["end"],
        model_version=filters["model_version"], refresh_nonce=nonce, repo_root=str(REPO_ROOT),
    )
    if preds.empty:
        _empty("predictions")
        return

    actual_col = "actual_log_return_lead1"

    # ── Regression ──────────────────────────────────────────────────────────
    st.subheader("Regression  ·  return_short / return_long")
    for col, label in [("return_short", "Short"), ("return_long", "Long")]:
        if col not in preds.columns or actual_col not in preds.columns:
            continue
        df = preds[[col, actual_col]].dropna()
        if df.empty:
            continue
        y_pred, y_true = df[col].values, df[actual_col].values
        st.markdown(f"**{label} horizon**")
        _metrics([("RMSE", _rmse(y_true, y_pred)), ("MAE", _mae(y_true, y_pred)), ("R²", _r2(y_true, y_pred))])

    st.divider()
    st.caption("Predicted vs Actual log return")
    _chart(charts.plot_predicted_vs_actual(preds))

    c1, c2 = st.columns(2)
    with c1:
        st.caption("Scatter: predicted vs actual")
        _chart(charts.plot_scatter_pred_vs_actual(preds))
    with c2:
        st.caption("Residual distribution")
        _chart(charts.plot_residual_distribution(preds))

    c3, c4 = st.columns(2)
    with c3:
        st.caption("Residuals over time")
        _chart(charts.plot_residuals(preds))
    with c4:
        st.caption("Rolling RMSE")
        _chart(charts.plot_rolling_rmse(preds))

    # ── Classification ───────────────────────────────────────────────────────
    st.divider()
    st.subheader("Classification  ·  sign_short / sign_long")
    threshold = float(model_config.get("targets", {}).get("sign", {}).get("threshold", 0.0015))

    for sign_col, label in [("sign_short", "Short"), ("sign_long", "Long")]:
        if sign_col not in preds.columns or actual_col not in preds.columns:
            continue
        df = preds[[sign_col, actual_col]].dropna()
        if df.empty:
            continue
        actual_sign = pd.cut(
            df[actual_col],
            bins=[-np.inf, -threshold, threshold, np.inf],
            labels=[0, 1, 2],
        ).astype(int)
        pred_sign = pd.to_numeric(df[sign_col], errors="coerce").round().astype(int)
        mask = actual_sign.notna() & pred_sign.notna()
        y_true, y_pred = actual_sign[mask].values, pred_sign[mask].values
        if len(y_true) == 0:
            continue

        st.markdown(f"**{label} horizon**")
        _metrics([("F1 macro", _f1_macro(y_true, y_pred))])

        c1, c2 = st.columns(2)
        with c1:
            st.caption("Rolling F1")
            _chart(charts.plot_rolling_f1(preds, col=f"f1_{sign_col}"))
        with c2:
            st.caption("Confusion matrix")
            _chart(charts.plot_confusion_matrix(preds, pred_col=sign_col, actual_col=actual_col))


# ---------------------------------------------------------------------------
# Tab 3 — Prediction Analysis
# ---------------------------------------------------------------------------

def _tab_prediction_analysis(data_config, model_config, filters, nonce):
    preds = get_predictions(
        data_config=data_config, model_config=model_config,
        symbol=filters["symbol"], start=filters["start"], end=filters["end"],
        model_version=filters["model_version"], refresh_nonce=nonce, repo_root=str(REPO_ROOT),
    )
    if preds.empty:
        _empty("predictions")
        return

    threshold = float(model_config.get("inference", {}).get("confidence_threshold", 0.0015))

    st.subheader("A · Prediction Distribution")
    _chart(charts.plot_prediction_distribution(preds))

    st.subheader("B · Confidence vs Directional Accuracy")
    if "direction_correct" in preds.columns:
        _chart(charts.plot_confidence_vs_accuracy(preds, threshold))
    else:
        _empty("direction_correct column — no actuals aligned yet")

    st.subheader("C · Directional Accuracy by Horizon")
    if "direction_correct" in preds.columns:
        _metrics([
            ("Overall dir. accuracy", f"{preds['direction_correct'].mean():.2%}"),
            ("N evaluated", int(preds["direction_correct"].notna().sum())),
        ])
    _chart(charts.plot_directional_accuracy_by_horizon(preds))

    st.subheader(f"D · PnL Simulation  (threshold = ±{threshold:.4f})")
    pnl = simulate_pnl(preds, confidence_threshold=threshold)
    if pnl["n_trades"] > 0:
        _metrics([
            ("Trades", pnl["n_trades"]),
            ("Hit ratio", f"{pnl['hit_ratio']:.2%}" if pnl["hit_ratio"] is not None else "N/A"),
            ("Approx. annualised Sharpe", f"{pnl['sharpe']:.2f}" if pnl["sharpe"] is not None else "N/A"),
        ])
        _chart(charts.plot_pnl_curve(pnl["cum_return"]))
    else:
        st.warning("No trades cleared the confidence threshold in this window.")


# ---------------------------------------------------------------------------
# Tab 4 — Feature Diagnostics
# ---------------------------------------------------------------------------

def _tab_features(data_config, model_config, filters, nonce):
    gold = get_feature_snapshot(
        data_config=data_config, model_config=model_config,
        symbol=filters["symbol"], start=filters["start"], end=filters["end"],
        refresh_nonce=nonce, repo_root=str(REPO_ROOT),
    )
    if gold.empty:
        _empty("gold feature data")
        return

    baseline = get_training_baseline_stats(model_config, repo_root=str(REPO_ROOT))
    feature_list = [
        f for f in model_config.get("features_long", model_config.get("features_short", []))
        if f != "symbol" and f in gold.columns
    ]
    target = model_config.get("target", "log_return_lead1")

    st.subheader("A · Feature Distributions  (Live vs Training baseline)")
    top10 = feature_list[:10]
    for i in range(0, len(top10), 2):
        cols = st.columns(2)
        for j, feat in enumerate(top10[i:i+2]):
            with cols[j]:
                _chart(charts.plot_feature_histogram_overlay(gold[feat], baseline, feat))

    st.subheader("B · Correlation Heatmap  (features → target)")
    _chart(charts.plot_correlation_heatmap(gold, target=target, features=feature_list[:20]))

    st.subheader("C · Missing & Invalid Data")
    c1, c2 = st.columns([3, 1])
    with c1:
        _chart(charts.plot_null_rates(gold[feature_list]))
    with c2:
        if "is_valid_feature_row" in gold.columns:
            valid_pct = gold["is_valid_feature_row"].mean() * 100
            _metrics([
                ("Valid rows", f"{valid_pct:.2f}%"),
                ("Invalid rows", int((~gold["is_valid_feature_row"].astype(bool)).sum())),
            ])

    st.subheader("D · Feature Importance")
    st.info(
        "Populated automatically when models expose `feature_importances` tags in MLflow. "
        "Run a training pipeline and check the registry."
    )


# ---------------------------------------------------------------------------
# Tab 5 — Drift & Retraining
# ---------------------------------------------------------------------------

def _tab_drift(model_config, filters, nonce):
    drift, drift_features = get_drift_history(
        model_config=model_config, start=filters["start"], end=filters["end"],
        refresh_nonce=nonce, repo_root=str(REPO_ROOT),
    )
    drift_cfg = model_config.get("monitoring", {}).get("drift", {}) or {}
    data_thr = float(drift_cfg.get("data_drift_threshold", 0.25))
    model_thr = float(drift_cfg.get("model_drift_threshold", 0.25))

    st.subheader("A · Drift Scores over Time")
    if drift.empty:
        _empty("drift history")
    else:
        latest = drift.sort_values("event_time").iloc[-1]
        _metrics([
            ("Overall drift", latest.get("drift_score")),
            ("Data drift", latest.get("data_drift_score")),
            ("Model drift", latest.get("model_drift_score")),
            ("Triggered", "Yes" if bool(latest.get("triggered", False)) else "No"),
        ])
        _chart(charts.plot_drift_scores(drift, data_thr, model_thr))

    st.subheader("B · Per-feature Drift  (top 20 by score)")
    if not drift_features.empty:
        sort_cols = [c for c in ["drift_score", "event_time"] if c in drift_features.columns]
        top = drift_features.sort_values(sort_cols, ascending=False).head(20)
        disp = [c for c in ["feature_name", "drift_score", "drift_detected", "event_time"] if c in top.columns]
        st.dataframe(top[disp], use_container_width=True, hide_index=True)
    else:
        _empty("per-feature drift data")

    st.subheader("C · Retraining Events")
    if not drift.empty and "triggered" in drift.columns:
        events = drift[drift["triggered"].fillna(False).astype(bool)]
        if not events.empty:
            disp = [c for c in ["event_time", "trigger_reason", "drift_score", "data_drift_score", "model_drift_score"]
                    if c in events.columns]
            st.dataframe(events[disp].sort_values("event_time", ascending=False),
                         use_container_width=True, hide_index=True)
            cooldown = int(
                model_config.get("monitoring", {}).get("retraining", {}).get("cooldown_minutes", 30)
            )
            st.caption(f"Cooldown guard: {cooldown} minutes between consecutive triggers.")
        else:
            st.success("No retraining events in this window.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    apply_page_config()
    apply_app_styles()

    data_config = load_yaml(DATA_CONFIG_PATH)
    model_config = load_yaml(MODEL_CONFIG_PATH)
    mlflow_config = load_yaml(MLFLOW_CONFIG_PATH)

    if "nonce" not in st.session_state:
        st.session_state["nonce"] = 0

    filters = render_sidebar_filters(
        data_config=data_config,
        refresh_nonce=st.session_state["nonce"],
        repo_root=str(REPO_ROOT),
    )
    if filters["refresh_clicked"]:
        st.session_state["nonce"] += 1
        st.rerun()

    nonce = st.session_state["nonce"]

    st.markdown(
        f"## ⚡ CryptoQuant · Model Observatory"
        f"<span style='font-size:0.85rem;color:#94A3B8;margin-left:12px'>"
        f"symbol: **{filters['symbol']}** &nbsp;|&nbsp; version: **{filters['model_version']}**"
        f"</span>",
        unsafe_allow_html=True,
    )

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview",
        "🎯 Model Performance",
        "🔍 Prediction Analysis",
        "🧬 Feature Diagnostics",
        "📡 Drift & Retraining",
    ])

    with tab1:
        _tab_overview(data_config, model_config, mlflow_config, filters, nonce)
    with tab2:
        _tab_model_performance(data_config, model_config, filters, nonce)
    with tab3:
        _tab_prediction_analysis(data_config, model_config, filters, nonce)
    with tab4:
        _tab_features(data_config, model_config, filters, nonce)
    with tab5:
        _tab_drift(model_config, filters, nonce)


if __name__ == "__main__":
    main()
