from __future__ import annotations

import argparse

from pipelines.jobs.sentiment.core import (
    run_bronze,
    run_gold,
    run_gold_processed,
    run_ingest,
    run_silver,
    validate_source,
)
from utils.logger import get_logger
from utils.spark_config import get_spark_app_name, get_spark_master
from utils.spark_utils import create_delta_spark_session

logger = get_logger("sentiment_pipeline_runner")


def _run_selected_stage(stage: str, mode: str, source: str, reprocess: bool = False) -> None:
    stage_suffix = f"-{source}" if stage == "ingest" and source != "all" else ""
    app_name = f"{get_spark_app_name()}-sentiment-{mode}-{stage}{stage_suffix}"
    master = get_spark_master()
    spark_conf: dict[str, str] | None = None
    if stage == "ingest" and mode in {"batch", "streaming"} and master.startswith("local["):
        # Airflow runs one ingest task per source in parallel; keep each Spark app small.
        master = "local[1]"
        spark_conf = {
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
        }
    spark = create_delta_spark_session(app_name, master=master, spark_conf=spark_conf)
    try:
        counts: dict[str, int] = {}
        if stage in {"ingest", "all"}:
            counts["ingest"] = run_ingest(spark, execution_mode=mode, source=source)
        if stage in {"bronze", "all"}:
            counts["bronze"] = run_bronze(spark, execution_mode=mode)
        if stage in {"silver", "all"}:
            counts["silver"] = run_silver(spark, execution_mode=mode)
        if stage in {"gold", "all"}:
            counts["gold"] = run_gold(spark, execution_mode=mode, reprocess=reprocess)
        if stage in {"gold_processed", "all"}:
            counts["gold_processed"] = run_gold_processed(spark, execution_mode=mode)
        logger.info("Sentiment run complete mode=%s stage=%s counts=%s", mode, stage, counts)
    finally:
        try:
            spark.stop()
        except Exception:
            logger.exception("Failed to stop Spark cleanly for app=%s", app_name)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run config-driven sentiment pipeline stages")
    parser.add_argument(
        "--stage",
        choices=["all", "ingest", "bronze", "silver", "gold", "gold_processed"],
        default="all",
        help="Which stage to run",
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "streaming"],
        default="batch",
        help="Execution mode that selects config and Delta targets",
    )
    parser.add_argument(
        "--source",
        default="all",
        help="Source selector for ingest stage: all, telegram, youtube, reddit, or news",
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="Force full recomputation for applicable stages by skipping incremental state filtering.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    _run_selected_stage(args.stage, args.mode, validate_source(args.source), reprocess=args.reprocess)
