import os
from typing import Dict, Optional

from prometheus_client import CollectorRegistry, push_to_gateway

from utils_global.config_loader import load_config
from utils_global.logger import get_logger


logger = get_logger(__name__)


def _monitoring_metrics_config() -> Dict:
    try:
        model_config = load_config("configs/model.yaml") or {}
        monitoring = model_config.get("monitoring") or {}
        return monitoring.get("metrics") or {}
    except Exception:
        return {}


def metric_name(base_metric: str) -> str:
    cfg = _monitoring_metrics_config()
    namespace = os.getenv("METRICS_NAMESPACE", cfg.get("namespace", "cryptoquant"))
    return f"{namespace}_{base_metric}"


def pushgateway_url() -> str:
    cfg = _monitoring_metrics_config()
    return os.getenv("PUSHGATEWAY_URL", cfg.get("pushgateway_url", "http://pushgateway:9091"))


def build_registry() -> CollectorRegistry:
    return CollectorRegistry()


def push_registry(
    registry: CollectorRegistry,
    job_name: str,
    grouping_key: Optional[Dict[str, str]] = None,
) -> None:
    url = pushgateway_url()
    try:
        push_to_gateway(url, job=job_name, registry=registry, grouping_key=grouping_key or {})
    except Exception as exc:
        logger.warning("Failed to push metrics to %s for job=%s: %s", url, job_name, exc)
