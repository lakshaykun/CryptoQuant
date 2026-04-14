# pipelines/utils/spark.py

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pipelines.utils.config_loader import load_config

_spark = None  # singleton


def get_spark(logger, need_kafka=False, force_new=False, config_path="configs/spark.yaml"):
    global _spark

    if _spark and not force_new:
        return _spark

    config = load_config(config_path)

    builder = (
        SparkSession.builder
        .appName(config["app_name"])
        .master(config["master"])
    )
    
    packages = []

    # delta always needed
    packages.append("io.delta:delta-spark_2.12:3.1.0")

    if need_kafka:
        packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")

    builder = builder.config("spark.jars.packages", ",".join(packages))

    # delta configs
    builder = (
        builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    # Apply other configs dynamically
    for key, value in config["spark_configs"].items():
        # avoid duplicate overwrite
        if key not in ["spark.jars.packages"]:
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        f"Spark started | Version: {spark.version} | Master: {spark.sparkContext.master}"
    )

    _spark = spark
    return spark