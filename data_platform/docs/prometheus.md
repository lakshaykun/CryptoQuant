# Prometheus Monitoring

CryptoQuant uses Prometheus as the operational metrics layer for the platform. The goal is not just to collect infrastructure health data, but to connect model-serving, streaming throughput, data drift, prediction drift, and retraining decisions into one observable control loop.

This document explains how Prometheus fits into the project, why each monitoring component exists, and how the current implementation works end to end.

## Why Prometheus in this project

CryptoQuant is a multi-stage MLOps system:

1. Binance data is ingested into Kafka.
2. Spark Streaming writes Bronze, Silver, and Gold Delta tables.
3. Batch jobs prepare training data and train models.
4. FastAPI serves online predictions.
5. Drift monitoring decides when the model should be retrained.

That pipeline can fail in several different ways:

- A container can crash or consume too much CPU or memory.
- Kafka or Spark can fall behind and build up lag.
- The API can continue serving requests while model quality quietly degrades.
- The feature distribution in Gold can drift away from the training baseline.
- Prediction error can increase even if the data pipeline is still healthy.

Prometheus gives the project a common way to observe all of those signals. It lets us:

- Scrape service-level metrics from HTTP endpoints.
- Collect host and container metrics from exporters.
- Push batch-job and Spark micro-batch metrics through Pushgateway.
- Evaluate alert rules that detect operational and model-health problems.
- Trigger downstream actions, including retraining, when drift is detected.

## Monitoring architecture

The monitoring stack is defined in [docker-compose.yml](../docker-compose.yml) and [monitoring/prometheus/prometheus.yml](../monitoring/prometheus/prometheus.yml).

The current components are:

- Prometheus server for scraping and alert evaluation.
- Pushgateway for short-lived jobs and Spark micro-batch metrics.
- Node Exporter for host-level CPU, memory, filesystem, and load metrics.
- cAdvisor for container-level CPU, memory, network, and process metrics.
- Kafka Exporter for broker and topic health.
- Airflow statsd-exporter for DAG and task metrics.
- FastAPI metrics endpoint at /metrics.

The scrape layout is intentionally mixed:

- Pull-based scraping for always-on services such as FastAPI, exporters, and Prometheus itself.
- Push-based publication for jobs that run in batches or micro-batches, such as Spark processing.

That split is important. Spark foreachBatch callbacks are not long-lived HTTP services, so Pushgateway is a better fit than trying to expose a server from each job.

## Conceptual model

Prometheus is built around a few core ideas.

### Metric types

- Counter: monotonically increasing value, used for request counts, prediction counts, and event totals.
- Gauge: value that can go up and down, used for lag, drift scores, and current DAG status.
- Histogram: distribution of values, used for request latency and prediction error spread.

### Labels

Labels are dimensions attached to metrics. In CryptoQuant they are used carefully to avoid high-cardinality explosion.

Examples:

- HTTP method, path, and status on API request counters.
- DAG id and status on Airflow run status metrics.
- Pipeline job name on Spark batch metrics.

Avoid using labels such as request id, symbol, or timestamp directly in Prometheus metrics. Those values create too many unique series and make Prometheus expensive to operate.

### Scrape interval

Prometheus regularly scrapes targets on a schedule. This project uses a 15 second scrape interval in [monitoring/prometheus/prometheus.yml](../monitoring/prometheus/prometheus.yml).

That means:

- Metrics reflect near-real-time state for services.
- Drift metrics do not need to be updated every second.
- Short-lived jobs should push metrics as part of their run, not rely on a scrape later.

## FastAPI instrumentation

The API is instrumented in [api/app.py](../api/app.py).

The exposed metrics are:

- cryptoquant_api_requests_total
- cryptoquant_api_request_latency_seconds
- cryptoquant_api_predictions_total
- cryptoquant_api_prediction_error
- cryptoquant_api_prediction_error_count_total
- cryptoquant_api_prediction_mae_last

### What they mean

- Requests total tracks all HTTP requests to the API.
- Latency measures how long the request lifecycle takes, including model load and inference.
- Prediction count tracks the number of prediction outputs returned.
- Prediction error tracks absolute error when actual values are included in the request payload.
- Prediction MAE last stores the most recent mean absolute error from a request that included ground truth.

### Why the API includes prediction error support

In many serving systems, prediction error is not available immediately because actual outcomes arrive later. CryptoQuant supports an optional actual value in the request schema so the API can record error when the caller already knows the realized target.

That is useful for:

- offline replay tests,
- shadow scoring,
- backfills,
- manual evaluation from labeled datasets.

For normal online predictions, the API still works as before and simply emits request and prediction metrics.

### /metrics endpoint

The API exposes /metrics using prometheus_client. Prometheus scrapes that endpoint directly.

This is the canonical pattern for a FastAPI service in Prometheus:

1. Instrument the application code.
2. Expose /metrics.
3. Let Prometheus scrape that endpoint.

## Spark monitoring

The streaming jobs live in [pipelines/jobs/streaming/spark_streaming.py](../pipelines/jobs/streaming/spark_streaming.py) and [pipelines/jobs/streaming/spark_predictions.py](../pipelines/jobs/streaming/spark_predictions.py).

Spark publishes the following metrics through Pushgateway:

- cryptoquant_records_processed
- cryptoquant_batch_processing_time_seconds
- cryptoquant_streaming_lag_seconds

### Why Pushgateway is used

Spark foreachBatch callbacks are not HTTP services. They are event-driven functions inside the streaming engine. Prometheus cannot scrape them directly unless you build a separate exporter process, which is unnecessary for this project.

Pushgateway is a better fit because the streaming job can:

1. Compute the metric at the end of the batch.
2. Push the values to a durable endpoint.
3. Allow Prometheus to scrape the pushed metrics later.

### Streaming lag

Streaming lag is especially important for this stack because a healthy model is only useful if the latest market data is reaching Gold and predictions on time.

Lag can indicate:

- Kafka backlog,
- Spark processing slowness,
- checkpoint issues,
- container resource pressure,
- upstream ingestion delays.

If lag becomes large, the model may score stale features and produce low-value predictions even if the job is technically still running.

## Airflow monitoring

Airflow metrics are surfaced through the statsd-exporter configured in [docker-compose.yml](../docker-compose.yml).

The custom project-level DAG callbacks live in [airflow/dags/monitoring_callbacks.py](../airflow/dags/monitoring_callbacks.py).

They publish:

- cryptoquant_airflow_dag_last_run_status
- cryptoquant_airflow_dag_duration_seconds

### Why this matters

Airflow already knows whether a DAG succeeded or failed, but those events are usually trapped in task logs and the UI. Prometheus makes them operational signals that can be alerted on and correlated with drift or throughput problems.

## Drift detection

The drift logic is implemented in [models/monitoring/drift.py](../models/monitoring/drift.py).

It computes two categories of drift:

### Data drift

Data drift compares a recent Gold window against the training baseline.

The current implementation looks at:

- mean shift,
- standard deviation shift,
- Population Stability Index, or PSI.

The comparison is performed over the feature set defined in [configs/model.yaml](../configs/model.yaml).

Data drift answers this question:

Is the market feature distribution currently different enough from the training distribution that the model may no longer generalize well?

### Prediction drift

Prediction drift compares model outputs against realized actuals when they are available.

The current implementation looks at:

- RMSE trend,
- RMSE ratio versus baseline,
- PSI on prediction values.

Prediction drift answers this question:

Are the model’s forecasts getting worse even if the data pipeline still looks healthy?

### Drift output

The drift job returns:

- drift_score,
- drift_detected,
- data_drift breakdown,
- prediction_drift breakdown.

These values are then exported to Prometheus as:

- cryptoquant_data_drift_score
- cryptoquant_prediction_drift_score
- cryptoquant_drift_alert
- cryptoquant_prediction_rmse_ratio
- cryptoquant_retraining_triggered

## Retraining trigger loop

When drift is detected, the drift monitor can trigger the model training DAG through the Airflow REST API.

The retraining logic is controlled by:

- airflow_api_url,
- dag_id,
- username and password,
- cooldown_minutes,
- state_path,
all defined in [configs/model.yaml](../configs/model.yaml).

### Why cooldown logic exists

Without cooldown protection, a noisy drift signal could trigger retraining repeatedly and overload the platform.

Cooldown prevents retraining storms by ensuring that a DAG is not fired again until the configured time window has elapsed.

### Retraining flow

1. The drift monitor reads Gold and prediction history.
2. It computes drift scores.
3. It exports metrics to Prometheus.
4. If drift is high enough and cooldown is inactive, it calls Airflow REST API.
5. Airflow starts model_training_pipeline.
6. The trigger event is persisted as state and added to drift history.

## Alert rules

Alert rules are defined in [monitoring/prometheus/alerts.yml](../monitoring/prometheus/alerts.yml).

The rules cover three operational layers:

### Model quality alerts

- High data drift.
- High prediction drift.
- Drift alert flag raised by the drift job.
- Prediction RMSE trend worsening.

### Serving alerts

- API prediction error spikes.

### Pipeline alerts

- Streaming lag too high.
- Airflow DAG failure.

Alerts are important because not every problem should be solved by automatic retraining. Some failures need immediate human attention, such as:

- service outages,
- resource exhaustion,
- broken data paths,
- Kafka or Spark backlogs,
- unexpected schema or model loading errors.

## Grafana compatibility

The stack is Grafana-friendly by design even though Grafana is not required for the current implementation.

Grafana can read the same Prometheus datasource and visualize:

- API traffic and latency,
- Spark batch time and lag,
- Kafka health,
- node and container resource usage,
- Airflow success/failure rates,
- drift score trends over time.

Recommended dashboard panels include:

- request rate and latency for the API,
- streaming lag over time,
- data drift and prediction drift side by side,
- latest DAG status and recent failures,
- container memory and CPU saturation.

## Operational runbook

Useful commands are documented in [docs/commands.md](commands.md).

Common checks:

1. Open Prometheus UI at http://localhost:9090.
2. Open FastAPI metrics at http://localhost:8000/metrics.
3. Confirm exporters are reachable on their service ports.
4. Inspect alert firing rules from the Prometheus rules page.
5. Check drift monitor logs if retraining did not trigger.

### What to look for first

If the model quality suddenly drops, check in this order:

1. Streaming lag.
2. Gold table freshness.
3. API request latency and error counts.
4. Prediction drift and RMSE ratio.
5. Airflow retraining triggers and cooldown state.

That ordering distinguishes infrastructure issues from model-quality issues quickly.

## Design notes

This implementation is intentionally modular.

- Monitoring concerns stay outside the training pipeline.
- Drift logic lives in its own module.
- Prometheus metric names are namespaced and config-driven.
- Retraining is triggered through a standard Airflow REST call rather than a custom one-off hook.
- The system can be extended later with Alertmanager, Grafana dashboards, or a custom model registry policy without reworking the core data flow.

## Related files

- [docker-compose.yml](../docker-compose.yml)
- [api/app.py](../api/app.py)
- [pipelines/jobs/streaming/spark_streaming.py](../pipelines/jobs/streaming/spark_streaming.py)
- [pipelines/jobs/streaming/spark_predictions.py](../pipelines/jobs/streaming/spark_predictions.py)
- [models/monitoring/drift.py](../models/monitoring/drift.py)
- [airflow/dags/drift_monitor_pipeline.py](../airflow/dags/drift_monitor_pipeline.py)
- [airflow/dags/monitoring_callbacks.py](../airflow/dags/monitoring_callbacks.py)
- [monitoring/prometheus/prometheus.yml](../monitoring/prometheus/prometheus.yml)
- [monitoring/prometheus/alerts.yml](../monitoring/prometheus/alerts.yml)
- [configs/model.yaml](../configs/model.yaml)
