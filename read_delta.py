import argparse
import glob
import os
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("ReadGoldAndExportCSV").getOrCreate()


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


def write_single_csv(df, output_csv: str) -> None:
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # CSV cannot store nested types directly, so serialize them to JSON strings.
    for field in df.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            df = df.withColumn(field.name, F.to_json(F.col(field.name)))

    with tempfile.TemporaryDirectory(prefix="spark_csv_tmp_") as tmp_dir:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)
        part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
        if not part_files:
            raise RuntimeError("Spark did not produce a CSV part file.")

        shutil.move(part_files[0], output_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read all parquet files from gold layer and export to one CSV file."
    )
    parser.add_argument(
        "--input",
        default="delta/gold",
        help="gold layer directory containing parquet files (default: delta/gold)",
    )
    parser.add_argument(
        "--output",
        default="delta/gold_all_data.csv",
        help="Output CSV file path (default: delta/gold_all_data.csv)",
    )
    args = parser.parse_args()

    spark = build_spark()
    try:
        parquet_files = list_data_parquet_files(args.input)
        print(f"Found {len(parquet_files)} parquet files under {args.input}")

        df = spark.read.parquet(*parquet_files)
        print("gold data schema:")
        df.printSchema()
        print(f"Row count: {df.count()}")

        write_single_csv(df, args.output)
        print(f"Single CSV created at: {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()