import argparse
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from wordcloud import STOPWORDS, WordCloud
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'wordcloud'. Install with: pip install wordcloud"
    ) from exc


EXTRA_STOPWORDS = {
    "btc",
    "bitcoin",
    "crypto",
    "cryptocurrency",
    "amp",
    "http",
    "https",
    "www",
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("sentiment-wordcloud")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .getOrCreate()
    )


def read_silver_df(spark: SparkSession, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)


def sanitize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = re.sub(r"[^a-z\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def collect_sentiment_text(df: DataFrame, sentiment: str, limit: int) -> str:
    sentiment = sentiment.lower().strip()
    sentiment_filter = None
    label_filter = None
    sign_filter = None

    if "label" in df.columns:
        label_value = F.lower(F.col("label"))
        if sentiment == "positive":
            label_filter = label_value.isin("positive", "pos", "label_1", "bullish", "1")
        elif sentiment == "negative":
            label_filter = label_value.isin("negative", "neg", "label_0", "bearish", "0")

    if "weighted_sentiment" in df.columns:
        if sentiment == "positive":
            sign_filter = F.col("weighted_sentiment") > F.lit(0)
        elif sentiment == "negative":
            sign_filter = F.col("weighted_sentiment") < F.lit(0)

    if label_filter is not None and sign_filter is not None:
        sentiment_filter = label_filter | sign_filter
    else:
        sentiment_filter = label_filter if label_filter is not None else sign_filter

    if sentiment_filter is None:
        return ""

    rows = (
        df.where(sentiment_filter)
        .select(F.col("text").cast("string").alias("text"))
        .where(F.col("text").isNotNull())
        .limit(limit)
        .collect()
    )

    cleaned = [sanitize_text(row["text"]) for row in rows if row["text"]]
    cleaned = [t for t in cleaned if t]
    return " ".join(cleaned)


def write_wordcloud(text_blob: str, title: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not text_blob.strip():
        plt.figure(figsize=(14, 8))
        plt.axis("off")
        plt.title(title)
        plt.text(0.5, 0.5, "No data available", ha="center", va="center", fontsize=20)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150)
        plt.close()
        return

    wc = WordCloud(
        width=1600,
        height=900,
        background_color="white",
        collocations=False,
        stopwords=set(STOPWORDS) | EXTRA_STOPWORDS,
        max_words=300,
    ).generate(text_blob)

    plt.figure(figsize=(14, 8))
    plt.imshow(wc, interpolation="bilinear")
    plt.title(title)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate positive/negative sentiment word clouds from silver Delta data."
    )
    parser.add_argument(
        "--input",
        default="delta/silver",
        help="Silver Delta path (default: delta/silver)",
    )
    parser.add_argument(
        "--output-dir",
        default="delta/wordclouds",
        help="Directory for output images (default: delta/wordclouds)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50000,
        help="Max rows per sentiment to sample (default: 50000)",
    )
    args = parser.parse_args()

    spark = build_spark()
    try:
        df = read_silver_df(spark, args.input)

        required_columns = {"text"}
        missing = required_columns.difference(df.columns)
        if missing:
            raise RuntimeError(f"Missing required columns in silver table: {sorted(missing)}")

        pos_text = collect_sentiment_text(df, "positive", args.limit)
        neg_text = collect_sentiment_text(df, "negative", args.limit)

        output_dir = Path(args.output_dir)
        pos_path = output_dir / "positive_wordcloud.png"
        neg_path = output_dir / "negative_wordcloud.png"

        write_wordcloud(pos_text, "Positive Sentiment Word Cloud", pos_path)
        write_wordcloud(neg_text, "Negative Sentiment Word Cloud", neg_path)

        print(f"Positive word cloud written to: {pos_path}")
        print(f"Negative word cloud written to: {neg_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()