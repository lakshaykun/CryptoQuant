import logging
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


logger = logging.getLogger(__name__)


def configure_job_logging(level: int = logging.INFO) -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
    else:
        root.setLevel(level)


def log_stream_schema(df: DataFrame, name: str) -> None:
    logger.info("Schema for %s:", name)
    df.printSchema()


def start_delta_query(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    query_name: str,
    output_mode: str = "append",
) -> StreamingQuery:
    logger.info(
        "Starting query=%s output=%s checkpoint=%s mode=%s",
        query_name,
        output_path,
        checkpoint_path,
        output_mode,
    )
    return (
        df.writeStream
        .format("delta")
        .queryName(query_name)
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
    )


def await_query(query: StreamingQuery) -> None:
    logger.info("Awaiting termination for query=%s", query.name)
    query.awaitTermination()
