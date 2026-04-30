import logging
import os
import socket
import threading
import time
from typing import Any
from urllib.parse import urlparse

import requests
from requests import exceptions as requests_exceptions
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


DEFAULT_LABEL_MAP: dict[str, float] = {
    "LABEL_1": 1.0,
    "LABEL_0": -1.0,
    "UNCERTAIN": 0.0,
}
MAX_API_TEXT_LENGTH = 5000


class CryptoBertClient:
    def __init__(self, endpoint: str, timeout_seconds: int = 10) -> None:
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self._endpoint_candidates = self._build_endpoint_candidates(endpoint)
        self._unreachable_endpoints: dict[str, float] = {}
        self._unreachable_ttl_seconds = max(5, int(os.getenv("CRYPTOBERT_ENDPOINT_TTL_SECONDS", "60")))
        self._circuit_open_until = 0.0
        self._consecutive_failures = 0
        self._circuit_failure_threshold = max(1, int(os.getenv("CRYPTOBERT_CIRCUIT_FAILURE_THRESHOLD", "5")))
        self._circuit_cooldown_seconds = max(5, int(os.getenv("CRYPTOBERT_CIRCUIT_COOLDOWN_SECONDS", "45")))
        self._min_request_interval_seconds = max(0.0, float(os.getenv("CRYPTOBERT_MIN_REQUEST_INTERVAL_SECONDS", "0.05")))
        self._last_request_monotonic = 0.0
        self._lock = threading.Lock()

        retries = Retry(
            total=1,
            connect=1,
            read=1,
            backoff_factor=0.2,
            status_forcelist=[429, 500, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def __getstate__(self) -> dict[str, Any]:
        """Make the client Spark-pickle-safe for executor shipping."""
        state = dict(self.__dict__)
        # These runtime objects are not pickleable; rebuild on restore.
        state["session"] = None
        state["_lock"] = None
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self.session = requests.Session()
        retries = Retry(
            total=1,
            connect=1,
            read=1,
            backoff_factor=0.2,
            status_forcelist=[429, 500, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self._lock = threading.Lock()

    def _wait_for_rate_limit(self) -> None:
        if self._min_request_interval_seconds <= 0:
            return

        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_monotonic
            remaining = self._min_request_interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
                now = time.monotonic()
            self._last_request_monotonic = now

    def _is_circuit_open(self) -> bool:
        return time.monotonic() < self._circuit_open_until

    def _mark_failure(self) -> None:
        with self._lock:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._circuit_failure_threshold:
                self._circuit_open_until = time.monotonic() + self._circuit_cooldown_seconds

    def _mark_success(self) -> None:
        with self._lock:
            self._consecutive_failures = 0
            self._circuit_open_until = 0.0

    @staticmethod
    def _build_endpoint_candidates(primary: str) -> list[str]:
        candidates: list[str] = []
        local_mode = str(os.getenv("RUN_ENV", "")).strip().lower() != "docker"
        primary_host = (urlparse(primary).hostname or "").lower()

        def _add(url: str) -> None:
            normalized = str(url or "").strip()
            if normalized and normalized not in candidates:
                candidates.append(normalized)

        # In local mode, deprioritize docker-only DNS names even if configured as primary.
        if not (local_mode and primary_host == "model_server"):
            _add(primary)
        # Allow explicit override chain via env in case deployments differ.
        for raw in str(os.getenv("CRYPTOBERT_ENDPOINT_FALLBACKS", "")).split(","):
            _add(raw)

        parsed = urlparse(primary)
        host = (parsed.hostname or "").lower()
        path = parsed.path or "/predict"
        if host in {"host.docker.internal", "model_server", "localhost", "127.0.0.1"}:
            # Prefer local loopback outside docker to avoid name-resolution failures.
            if local_mode:
                _add(f"http://127.0.0.1:8000{path}")
                _add(f"http://localhost:8000{path}")
                _add(f"http://host.docker.internal:8000{path}")
                _add(f"http://model_server:8000{path}")
            else:
                _add(f"http://model_server:8000{path}")
                _add(f"http://host.docker.internal:8000{path}")
                _add(f"http://127.0.0.1:8000{path}")
                _add(f"http://localhost:8000{path}")

        if local_mode and primary_host == "model_server":
            _add(primary)

        return candidates

    @staticmethod
    def _host_resolves(endpoint: str) -> bool:
        host = (urlparse(endpoint).hostname or "").strip()
        if not host:
            return False
        try:
            socket.getaddrinfo(host, None)
            return True
        except socket.gaierror:
            return False

    def _is_huggingface_endpoint(self) -> bool:
        parsed = urlparse(self.endpoint)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        return "huggingface.co" in host or "/hf-inference/" in path or "/models/" in path

    def _payload_for_endpoint(self, text: str, endpoint: str) -> dict[str, Any]:
        if self._is_huggingface_endpoint_url(endpoint):
            return {"inputs": text}
        return {"text": text}

    @staticmethod
    def _is_huggingface_endpoint_url(endpoint: str) -> bool:
        parsed = urlparse(endpoint)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        return "huggingface.co" in host or "/hf-inference/" in path or "/models/" in path

    def _headers_for_endpoint(self, endpoint: str) -> dict[str, str] | None:
        if not self._is_huggingface_endpoint_url(endpoint):
            return None

        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "x-wait-for-model": "true",
        }
        token = str(os.getenv("HUGGINGFACE_API_KEY", "")).strip().strip('"').strip("'")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @staticmethod
    def _normalize_label(label: str) -> str:
        value = str(label or "").strip().upper()
        if value in {"LABEL_1", "POSITIVE", "BULLISH"}:
            return "LABEL_1"
        if value in {"LABEL_0", "NEGATIVE", "BEARISH"}:
            return "LABEL_0"
        return "UNCERTAIN"

    @staticmethod
    def _parse_response(payload: Any) -> tuple[str, float]:
        if isinstance(payload, dict):
            if "label" in payload:
                normalized = CryptoBertClient._normalize_label(str(payload.get("label", "UNCERTAIN")))
                return normalized, float(payload.get("confidence", payload.get("score", 0.0)))

            predictions = payload.get("predictions")
            if isinstance(predictions, list) and predictions:
                dict_predictions = [item for item in predictions if isinstance(item, dict)]
                if dict_predictions:
                    top = max(dict_predictions, key=lambda item: float(item.get("score", 0.0)))
                    normalized = CryptoBertClient._normalize_label(str(top.get("label", "UNCERTAIN")))
                    return normalized, float(top.get("score", 0.0))

        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, list):
                candidates = [item for item in first if isinstance(item, dict)]
            else:
                candidates = [item for item in payload if isinstance(item, dict)]
            if candidates:
                top = max(candidates, key=lambda item: float(item.get("score", item.get("confidence", 0.0))))
                normalized = CryptoBertClient._normalize_label(str(top.get("label", "UNCERTAIN")))
                return normalized, float(top.get("score", top.get("confidence", 0.0)))

        return ("UNCERTAIN", 0.0)

    def infer_sentiment(self, text: str) -> tuple[str, float, float]:
        try:
            normalized_text = str(text or "").strip()
            if len(normalized_text) > MAX_API_TEXT_LENGTH:
                normalized_text = normalized_text[:MAX_API_TEXT_LENGTH]

            last_exc: Exception | None = None
            response = None
            result: Any = None

            for endpoint in self._endpoint_candidates:
                blocked_until = self._unreachable_endpoints.get(endpoint, 0.0)
                if blocked_until > time.monotonic():
                    continue
                if not self._host_resolves(endpoint):
                    self._unreachable_endpoints[endpoint] = time.monotonic() + self._unreachable_ttl_seconds
                    logger.info("CryptoBERT skipping unresolved endpoint=%s", endpoint)
                    continue
                try:
                    if self._is_circuit_open():
                        raise RuntimeError("CryptoBERT circuit open due to repeated upstream failures")

                    payload = self._payload_for_endpoint(normalized_text, endpoint)
                    headers = self._headers_for_endpoint(endpoint)
                    logger.info("CryptoBERT request endpoint=%s payload_keys=%s", endpoint, list(payload.keys()))
                    self._wait_for_rate_limit()
                    response = self.session.post(
                        endpoint,
                        json=payload,
                        headers=headers,
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    result = response.json()
                    if endpoint != self.endpoint:
                        logger.info("CryptoBERT switched endpoint from %s to %s", self.endpoint, endpoint)
                        self.endpoint = endpoint
                    logger.info("CryptoBERT response endpoint=%s status=%s", endpoint, response.status_code)
                    self._mark_success()
                    break
                except Exception as exc:
                    last_exc = exc
                    error_text = str(exc).lower()
                    if (
                        isinstance(exc, requests_exceptions.RetryError)
                        and ("too many 502" in error_text or " 502 " in error_text)
                    ):
                        # Avoid hammering an endpoint that's repeatedly returning 502.
                        self._unreachable_endpoints[endpoint] = time.monotonic() + self._unreachable_ttl_seconds
                    if "502" in error_text or "bad gateway" in error_text:
                        self._unreachable_endpoints[endpoint] = time.monotonic() + self._unreachable_ttl_seconds
                    self._mark_failure()
                    logger.warning("CryptoBERT endpoint attempt failed endpoint=%s error=%s", endpoint, str(exc))

            if result is None:
                if last_exc is not None:
                    raise last_exc
                raise RuntimeError("No CryptoBERT endpoint candidates available")

            label, confidence = self._parse_response(result)
            base_score = DEFAULT_LABEL_MAP.get(label, 0.0)
            return (label, confidence, base_score)
        except Exception as exc:
            logger.warning(
                "Sentiment inference failed endpoint=%s timeout_seconds=%s error=%s; falling back to UNCERTAIN",
                self.endpoint,
                self.timeout_seconds,
                str(exc),
            )
            return ("UNCERTAIN", 0.0, 0.0)
