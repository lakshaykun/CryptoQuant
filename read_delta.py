import argparse
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ReadGoldAndPlotSentiment")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def list_data_parquet_files(base_dir: str) -> list[str]:
    root = Path(base_dir)
    if not root.exists():
        raise FileNotFoundError(f"Input path does not exist: {base_dir}")

    # Ignore Delta transaction logs and checksum files.
    parquet_files = [
        str(p)
        for p in root.rglob("*.parquet")
        if "_delta_log" not in p.parts and not p.name.endswith(".crc")
    ]
    parquet_files.sort()

    if not parquet_files:
        raise FileNotFoundError(f"No parquet data files found under: {base_dir}")
    return parquet_files


def read_gold_df(spark: SparkSession, input_path: str):
    # Prefer native Delta read; fall back to parquet files for compatibility.
    try:
        return spark.read.format("delta").load(input_path)
    except Exception:
        parquet_files = list_data_parquet_files(input_path)
        return spark.read.parquet(*parquet_files)


def resolve_time_column(df) -> F.Column:
    for field in df.schema.fields:
        if field.name == "window" and isinstance(field.dataType, StructType):
            nested_names = {nested.name for nested in field.dataType.fields}
            if "end" in nested_names:
                return F.col("window.end")
            if "start" in nested_names:
                return F.col("window.start")

    for candidate in ("time", "timestamp", "event_time", "window"):
        if candidate in df.columns:
            return F.col(candidate)

    raise ValueError(
        "Could not find a time column. Expected one of: window, time, timestamp, event_time"
    )


def resolve_sentiment_column(df) -> F.Column:
    for candidate in ("total_sentiment_score", "total_sentiment", "sentiment_index"):
        if candidate in df.columns:
            return F.col(candidate)

    raise ValueError(
        "Could not find a sentiment column. Expected one of: total_sentiment_score, total_sentiment, sentiment_index"
    )


def plot_sentiment_line(df, output_plot: str, last_hours: int = 12) -> None:
    output_path = Path(output_plot)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_csv = output_path.with_suffix(".csv")

    time_col = resolve_time_column(df)
    sentiment_col = resolve_sentiment_column(df)

    minute_df = (
        df.select(
            F.to_timestamp(time_col).alias("plot_time"),
            sentiment_col.cast("double").alias("total_sentiment_score"),
        )
        .where(F.col("plot_time").isNotNull() & F.col("total_sentiment_score").isNotNull())
        .where(F.col("plot_time") >= F.expr(f"current_timestamp() - INTERVAL {last_hours} HOURS"))
        .where(F.col("plot_time") <= F.current_timestamp())
        .withColumn("minute_time", F.date_trunc("minute", F.col("plot_time")))
        .groupBy("minute_time")
        .agg(F.avg("total_sentiment_score").alias("minute_sentiment_score"))
        .orderBy("minute_time")
    )

    rows = minute_df.collect()
    if not rows:
        raise RuntimeError(
            f"No rows available to plot in the last {last_hours} hours."
        )

    observed_by_minute = {
        row["minute_time"]: float(row["minute_sentiment_score"])
        for row in rows
    }

    now_minute = datetime.now().replace(second=0, microsecond=0)
    start_minute = now_minute - timedelta(hours=last_hours)

    x_vals = []
    minute_scores = []
    is_observed_flags = []

    current_minute = start_minute
    while current_minute <= now_minute:
        x_vals.append(current_minute)
        if current_minute in observed_by_minute:
            minute_scores.append(observed_by_minute[current_minute])
            is_observed_flags.append(True)
        else:
            minute_scores.append(None)
            is_observed_flags.append(False)
        current_minute += timedelta(minutes=1)

    known_indices = [i for i, val in enumerate(minute_scores) if val is not None]
    if not known_indices:
        raise RuntimeError(
            f"No rows available to plot in the last {last_hours} hours."
        )

    for idx in range(len(minute_scores)):
        if minute_scores[idx] is not None:
            continue

        prev_idx = None
        next_idx = None

        for i in range(idx - 1, -1, -1):
            if minute_scores[i] is not None:
                prev_idx = i
                break

        for i in range(idx + 1, len(minute_scores)):
            if minute_scores[i] is not None:
                next_idx = i
                break

        if prev_idx is not None and next_idx is not None:
            # Linear interpolation between nearest known points.
            prev_val = minute_scores[prev_idx]
            next_val = minute_scores[next_idx]
            ratio = (idx - prev_idx) / (next_idx - prev_idx)
            minute_scores[idx] = prev_val + ratio * (next_val - prev_val)
        elif prev_idx is not None:
            minute_scores[idx] = minute_scores[prev_idx]
        elif next_idx is not None:
            minute_scores[idx] = minute_scores[next_idx]
        else:
            minute_scores[idx] = 0.0

    plt.figure(figsize=(12, 6))
    plt.plot(x_vals, minute_scores, linewidth=1.8, alpha=0.85, label="1-min sentiment (actual + predicted)")
    plt.title(f"Total Sentiment Score (Last {last_hours} Hours, 1-Min Aggregation)")
    plt.xlabel("Time")
    plt.ylabel("Total Sentiment Score")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    with output_csv.open("w", encoding="utf-8") as f:
        f.write("minute_time,sentiment_score,is_observed\n")
        for minute_time, score, observed in zip(x_vals, minute_scores, is_observed_flags):
            f.write(
                f"{minute_time.isoformat()},{score:.6f},{str(observed).lower()}\n"
            )

    print(f"Final 1-minute data written to: {output_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read gold Delta layer and plot total sentiment score over time."
    )
    parser.add_argument(
        "--input",
        default="delta/gold",
        help="Gold layer Delta table path (default: delta/gold)",
    )
    parser.add_argument(
        "--output",
        default="delta/gold_sentiment_line.png",
        help="Output plot file path (default: delta/gold_sentiment_line.png)",
    )
    parser.add_argument(
        "--last-hours",
        type=int,
        default=12,
        help="Only include data from the last N hours (default: 12)",
    )
    args = parser.parse_args()

    spark = build_spark()
    try:
        df = read_gold_df(spark, args.input)
        print("gold data schema:")
        df.printSchema()
        print(f"Row count: {df.count()}")

        plot_sentiment_line(df, args.output, args.last_hours)
        print(f"Line graph created at: {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()