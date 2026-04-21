import argparse
import multiprocessing as mp
import signal
import time
from collections.abc import Callable

from pipelines.ingestion.streaming.sentiment.news_stream_job import run_forever as run_news
from pipelines.ingestion.streaming.sentiment.reddit_stream_job import run_forever as run_reddit
from pipelines.ingestion.streaming.sentiment.telegram_stream_job import run_forever as run_telegram
from pipelines.ingestion.streaming.sentiment.youtube_stream_job import run_forever as run_youtube
from utils.logger import get_logger

logger = get_logger("sentiment_orchestrator")


def _spawn(name: str, target: Callable[..., None], interval_seconds: int) -> mp.Process:
    process = mp.Process(
        name=name,
        target=target,
        kwargs={"interval_seconds": interval_seconds},
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
    reddit_interval_seconds: int = 300,
    youtube_interval_seconds: int = 180,
    news_interval_seconds: int = 120,
    telegram_interval_seconds: int = 180,
) -> None:
    processes = [
        _spawn("reddit-producer", run_reddit, reddit_interval_seconds),
        _spawn("youtube-producer", run_youtube, youtube_interval_seconds),
        _spawn("news-producer", run_news, news_interval_seconds),
        _spawn("telegram-producer", run_telegram, telegram_interval_seconds),
    ]

    shutting_down = False

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
            time.sleep(1)
            dead = [process for process in processes if process.exitcode is not None]
            if dead:
                for process in dead:
                    logger.error(
                        "%s exited unexpectedly with code=%s",
                        process.name,
                        process.exitcode,
                    )
                shutting_down = True
                _stop_all(processes, "one or more producer processes exited")
                raise RuntimeError("Sentiment orchestrator stopped due to producer failure")
    finally:
        if any(process.is_alive() for process in processes):
            _stop_all(processes, "final cleanup")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run reddit, youtube, news, and telegram sentiment producers together",
    )
    parser.add_argument("--reddit-interval", type=int, default=300)
    parser.add_argument("--youtube-interval", type=int, default=180)
    parser.add_argument("--news-interval", type=int, default=120)
    parser.add_argument("--telegram-interval", type=int, default=180)
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
