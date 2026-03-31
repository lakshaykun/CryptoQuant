from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

def get_spark():
    builder = (
        SparkSession.builder
        .appName("crypto-mlops")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark

def load_silver_data():
    spark = get_spark()

    df = spark.read.format("delta").load("medallion/silver/market")

    return df.toPandas()