# pipelines/utils/spark.py

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pipelines.utils.config_loader import load_config

_spark = None  # singleton


def get_spark(logger, config_path="configs/spark.yaml"):
    global _spark

    if _spark:
        return _spark

    config = load_config(config_path)

    builder = (
        SparkSession.builder
        .appName(config["app_name"])
        .master(config["master"])
    )

    # Apply configs dynamically
    for key, value in config["spark_configs"].items():
        builder = builder.config(key, value)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        f"Spark started | Version: {spark.version} | Master: {spark.sparkContext.master}"
    )

    _spark = spark
    return spark