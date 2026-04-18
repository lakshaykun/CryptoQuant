import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


DEFAULT_LABEL_MAP: dict[str, float] = {
    "LABEL_1": 1.0,
    "LABEL_0": -1.0,
    "UNCERTAIN": 0.0,
}


class CryptoBertClient:
    def __init__(self, endpoint: str, timeout_seconds: int = 10) -> None:
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

        retries = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def infer_sentiment(self, text: str) -> tuple[str, float, float]:
        try:
            response = self.session.post(
                self.endpoint,
                json={"text": text},
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            result: dict[str, Any] = response.json()

            label = str(result.get("label", "UNCERTAIN"))
            confidence = float(result.get("confidence", 0.0))
            base_score = DEFAULT_LABEL_MAP.get(label, 0.0)
            return (label, confidence, base_score)
        except Exception:
            logger.debug("Sentiment inference failed; falling back to UNCERTAIN", exc_info=True)
            return ("UNCERTAIN", 0.0, 0.0)
