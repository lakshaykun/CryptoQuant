from __future__ import annotations

import argparse
import os
import socket
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()
from pipelines.jobs.sentiment.config import load_sentiment_pipeline_config
from utils.config_loader import load_config


@dataclass
class CheckResult:
    source: str
    ok: bool
    message: str


def _dns_check(host: str, port: int = 443, timeout: float = 3.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, "reachable"
    except Exception as exc:
        return False, f"unreachable ({exc})"


def _check_youtube() -> CheckResult:
    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        return CheckResult("youtube", False, "missing env YOUTUBE_API_KEY")
    ok, msg = _dns_check("youtube.googleapis.com", 443)
    return CheckResult("youtube", ok, msg if ok else f"blocked: {msg}")


def _check_reddit() -> CheckResult:
    ok, msg = _dns_check("www.reddit.com", 443)
    return CheckResult("reddit", ok, msg if ok else f"blocked: {msg}")


def _check_news() -> CheckResult:
    # Validate one representative host plus cryptopanic.
    ok_rss, msg_rss = _dns_check("cointelegraph.com", 443)
    ok_cp, msg_cp = _dns_check("cryptopanic.com", 443)
    if ok_rss and ok_cp:
        return CheckResult("news", True, "reachable")
    return CheckResult(
        "news",
        False,
        f"blocked: rss={msg_rss}, cryptopanic={msg_cp}",
    )


def _check_telegram() -> CheckResult:
    api_id = os.getenv("TELEGRAM_API_ID", "").strip()
    api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
    if not api_id or not api_hash:
        return CheckResult("telegram", False, "missing env TELEGRAM_API_ID/TELEGRAM_API_HASH")
    ok, msg = _dns_check("api.telegram.org", 443)
    return CheckResult("telegram", ok, msg if ok else f"blocked: {msg}")


def _check_model_endpoint() -> CheckResult:
    spark_cfg = load_config("configs/spark.yaml") or {}
    gold_cfg = spark_cfg.get("gold", {}) if isinstance(spark_cfg, dict) else {}
    endpoint = str(gold_cfg.get("sentiment_endpoint", "http://model-server:8000/predict"))
    host = endpoint.replace("http://", "").replace("https://", "").split("/")[0].split(":")[0]
    ok, msg = _dns_check(host, 8000 if host == "model-server" else 443)
    return CheckResult("model_endpoint", ok, msg if ok else f"blocked: {msg}")


def run_preflight(mode: str) -> int:
    config = load_sentiment_pipeline_config(mode)
    available_checks = {
        "reddit": _check_reddit,
        "youtube": _check_youtube,
        "news": _check_news,
        "telegram": _check_telegram,
    }
    checks = [available_checks[source]() for source in config.sources if source in available_checks]

    failed = [c for c in checks if not c.ok]
    print(f"[sentiment-preflight] mode={mode}")
    for check in checks:
        status = "OK" if check.ok else "BLOCKED"
        print(f" - {check.source}: {status} ({check.message})")

    if failed:
        blocked_sources = ", ".join(f.source for f in failed)
        print(f"[sentiment-preflight] FAIL: blocked sources/services -> {blocked_sources}")
        return 2

    print("[sentiment-preflight] PASS: all required sources/services reachable")
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sentiment source preflight checks")
    parser.add_argument("--mode", choices=["batch", "streaming"], default="streaming")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(run_preflight(args.mode))
