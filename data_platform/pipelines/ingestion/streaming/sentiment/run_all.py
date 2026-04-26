import argparse
import multiprocessing as mp
from queue import Empty
import signal
import time
from datetime import datetime, timezone
from collections.abc import Callable

from pipelines.ingestion.streaming.sentiment.news_stream_job import run_forever as run_news
from pipelines.ingestion.streaming.sentiment.reddit_stream_job import run_forever as run_reddit
from pipelines.ingestion.streaming.sentiment.telegram_stream_job import run_forever as run_telegram
from pipelines.ingestion.streaming.sentiment.youtube_stream_job import run_forever as run_youtube
from utils.logger import get_logger

logger = get_logger("sentiment_orchestrator")


def _spawn(
    name: str,
    target: Callable[..., None],
    interval_seconds: int,
    stats_queue: mp.Queue,
) -> mp.Process:
    process = mp.Process(
        name=name,
        target=target,
        kwargs={"interval_seconds": interval_seconds, "stats_queue": stats_queue},
        daemon=False,
    )
    process.start()
    logger.info("Started %s (pid=%s)", name, process.pid)
    return process


def _stop_all(processes: list[mp.Process], reason: str) -> None:
    logger.info("Stopping sentiment producer processes: %s", reason)
    for process in processes:
        if process.is_alive():
            process.terminate()

    for process in processes:
        process.join(timeout=10)
        if process.is_alive():
            logger.warning("Force killing %s (pid=%s)", process.name, process.pid)
            process.kill()
            process.join(timeout=5)


def run_all(
    reddit_interval_seconds: int = 60,
    youtube_interval_seconds: int = 60,
    news_interval_seconds: int = 60,
    telegram_interval_seconds: int = 60,
) -> None:
    stats_queue: mp.Queue = mp.Queue()
    process_specs = [
        ("reddit-producer", run_reddit, reddit_interval_seconds),
        ("youtube-producer", run_youtube, youtube_interval_seconds),
        ("news-producer", run_news, news_interval_seconds),
        ("telegram-producer", run_telegram, telegram_interval_seconds),
    ]
    processes = [
        _spawn(name, target, interval, stats_queue)
        for name, target, interval in process_specs
    ]

    shutting_down = False
    minute_counts = {"reddit": 0, "youtube": 0, "news": 0, "telegram": 0}
    current_minute = int(time.time() // 60)

    def _handle_signal(signum, _frame):
        nonlocal shutting_down
        if shutting_down:
            return
        shutting_down = True
        _stop_all(processes, f"received signal {signum}")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        while not shutting_down:
            now_minute = int(time.time() // 60)
            while True:
                try:
                    source, count, _ = stats_queue.get_nowait()
                except Empty:
                    break
                minute_counts[source] = minute_counts.get(source, 0) + int(count)

            if now_minute != current_minute:
                logger.info(
                    "[%s UTC] per-minute ingest counts: reddit=%d youtube=%d news=%d telegram=%d total=%d",
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    minute_counts.get("reddit", 0),
                    minute_counts.get("youtube", 0),
                    minute_counts.get("news", 0),
                    minute_counts.get("telegram", 0),
                    sum(minute_counts.values()),
                )
                minute_counts = {"reddit": 0, "youtube": 0, "news": 0, "telegram": 0}
                current_minute = now_minute

            time.sleep(1)
            dead = [process for process in processes if process.exitcode is not None]
            if dead:
                if shutting_down:
                    break
                for process in dead:
                    logger.error(
                        "%s exited unexpectedly with code=%s",
                        process.name,
                        process.exitcode,
                    )
                for process in dead:
                    spec = next((item for item in process_specs if item[0] == process.name), None)
                    if spec is None:
                        continue
                    name, target, interval = spec
                    replacement = _spawn(name, target, interval, stats_queue)
                    processes = [replacement if p.name == name else p for p in processes]
                    logger.warning("Restarted %s after unexpected exit", name)
    finally:
        if any(process.is_alive() for process in processes):
            _stop_all(processes, "final cleanup")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run reddit, youtube, news, and telegram sentiment producers together",
    )
    parser.add_argument("--reddit-interval", type=int, default=60)
    parser.add_argument("--youtube-interval", type=int, default=60)
    parser.add_argument("--news-interval", type=int, default=60)
    parser.add_argument("--telegram-interval", type=int, default=60)
    return parser.parse_args()


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    args = _parse_args()
    run_all(
        reddit_interval_seconds=max(1, args.reddit_interval),
        youtube_interval_seconds=max(1, args.youtube_interval),
        news_interval_seconds=max(1, args.news_interval),
        telegram_interval_seconds=max(1, args.telegram_interval),
    )
