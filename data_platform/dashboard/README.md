# CryptoQuant Dashboard

This dashboard is a Streamlit-based internal monitoring and research interface for CryptoQuant.

It now uses a dark, glassy, gradient-heavy visual system intended to feel like a polished internal MLOps console.

It is designed as a data-first operational view for:

- market and medallion pipeline visibility,
- model behavior diagnostics,
- drift monitoring,
- retraining lifecycle visibility.

## What the dashboard includes

- Global filters in the sidebar:
  - Symbol
  - Time range (15m, 1h, 6h, 24h, custom)
  - Model version
- Auto refresh every 30 seconds with filter state preserved.
- Manual refresh button.
- Hero banner with model registry and MLflow tracking context.
- Platform snapshot:
  - Target, feature count, drift thresholds, and retraining cooldown.
  - Latest Gold market snapshot.
  - Latest drift posture and retraining state.
- Overview metrics:
  - Latest price
  - Latest prediction
  - Drift score
  - Latest model RMSE / directional accuracy from MLflow
- Data pipeline views:
  - Gold ingestion rate
  - Gold row growth trend
  - Latest Gold update table
- Model behavior views:
  - Predicted vs actual
  - Actual vs predicted close price
  - Residuals
  - Rolling RMSE
- Drift monitoring from Delta history, including retraining trigger markers.
- Drift feature ranking and retraining trigger reasons from Delta history.
- Latest MLflow run metrics for model quality context.

## Module layout

- app.py: Dashboard orchestrator and section rendering.
- settings.py: App constants, page config, style, YAML loading.
- filters.py: Sidebar controls and time-window logic.
- helpers.py: Common utility conversions and path resolution.
- delta_client.py: Delta Lake access and model version loading.
- data_service.py: Data assembly for dashboard sections.
- mlflow_client.py: lightweight MLflow API lookups.
- charts.py: Plotly chart builders.

## Requirements

Install dashboard dependencies:

```bash
pip install -r dashboard/requirements.txt
```

## Run

From repository root:

```bash
streamlit run dashboard/app.py
```

Containerized:

```bash
docker compose up dashboard
```

Open:

- http://localhost:8501

## Notes

- The dashboard resolves /opt/app paths to local project paths automatically for Docker/local compatibility.
- The dashboard container is built from [docker/dashboard/Dockerfile](../docker/dashboard/Dockerfile) for CI/CD and compose use.
- Delta tables and MLflow responses are cached with short TTLs for responsive rendering.
- The theme settings live in `.streamlit/config.toml` and must keep `chartSequentialColors` at 10 values for Streamlit compatibility.
