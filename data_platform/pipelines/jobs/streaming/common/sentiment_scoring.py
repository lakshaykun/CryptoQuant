import concurrent.futures
from collections.abc import Iterator

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.jobs.streaming.common.sentiment_client import CryptoBertClient


SCORED_SENTIMENT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("text", StringType()),
    StructField("engagement", IntegerType()),
    StructField("symbol", StringType()),
    StructField("event_time", TimestampType()),
    StructField("label", StringType()),
    StructField("confidence", DoubleType()),
    StructField("weighted_sentiment", DoubleType()),
])


def score_sentiment_dataframe(
    df: DataFrame,
    client: CryptoBertClient,
    max_concurrent_requests: int = 2,
    chunk_size: int = 25,
) -> DataFrame:
    if df is None or df.rdd.isEmpty():
        return df.sparkSession.createDataFrame([], SCORED_SENTIMENT_SCHEMA)

    def _score_row_batch(rows: list) -> list[tuple]:
        if not rows:
            return []

        texts = [str(getattr(row, "text", "") or "") for row in rows]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_requests) as executor:
            results = list(executor.map(client.infer_sentiment, texts))

        scored_rows: list[tuple] = []
        for row, (label, confidence, base_score) in zip(rows, results):
            weighted_sentiment = float(base_score) * float(confidence)
            scored_rows.append(
                (
                    getattr(row, "id", None),
                    getattr(row, "timestamp", None),
                    getattr(row, "source", None),
                    getattr(row, "text", None),
                    getattr(row, "engagement", None),
                    getattr(row, "symbol", None),
                    getattr(row, "event_time", None),
                    label,
                    float(confidence),
                    weighted_sentiment,
                )
            )
        return scored_rows

    def _score_partition(rows_iter: Iterator) -> Iterator[tuple]:
        batch: list = []
        for row in rows_iter:
            batch.append(row)
            if len(batch) >= chunk_size:
                for scored in _score_row_batch(batch):
                    yield scored
                batch.clear()

        if batch:
            for scored in _score_row_batch(batch):
                yield scored

    scored_rdd = df.rdd.mapPartitions(_score_partition)
    return df.sparkSession.createDataFrame(scored_rdd, schema=SCORED_SENTIMENT_SCHEMA)
