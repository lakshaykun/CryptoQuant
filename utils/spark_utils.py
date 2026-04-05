import os
import sys

python_exec = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exec
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

from pyspark.sql import SparkSession
import pyspark

from delta import configure_spark_with_delta_pip


def create_delta_spark_session(
	app_name: str,
	include_kafka: bool = False,
	master: str | None = None,
) -> SparkSession:
	# Keep driver and worker Python versions aligned for PySpark UDF execution.

	builder = SparkSession.builder.appName(app_name)
	if master:
		builder = builder.master(master)

	builder = (
		builder
		.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
		.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
		.config("spark.sql.adaptive.enabled", "false")
		.config("spark.sql.debug.maxToStringFields", "200")
		.config("spark.pyspark.python", python_exec)
		.config("spark.pyspark.driver.python", python_exec)
		.config("spark.executorEnv.PYSPARK_PYTHON", python_exec)
	)
	extra_packages: list[str] = []

	if include_kafka:
		# Match connector version to installed PySpark version for compatibility.
		spark_version = pyspark.__version__.split("+")[0]
		kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"
		token_pkg = f"org.apache.spark:spark-token-provider-kafka-0-10_2.12:{spark_version}"
		extra_packages = [kafka_pkg, token_pkg]

	return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()