# pipelines/validation/silver/market.py

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame


class SilverMarketValidator:
    @staticmethod
    def validate(df: SparkDataFrame) -> dict:
        """
        Validates final silver data.
        """

        if df is None:
            return {"is_valid": False, "errors": ["DataFrame is None"]}

        if df.limit(1).count() == 0:
            return {"is_valid": False, "errors": ["DataFrame is empty"]}

        # ---------------------------
        # ONLY invariants (NOT cleaning logic)
        # ---------------------------

        agg = df.agg(
            F.sum(F.col("symbol").isNull().cast("int")).alias("symbol_nulls"),
            F.sum(F.col("timestamp").isNull().cast("int")).alias("timestamp_nulls"),
            F.sum((F.col("volume") < 0).cast("int")).alias("negative_volume"),
        ).collect()[0].asDict()

        errors = []

        if agg["symbol_nulls"] > 0:
            errors.append("Null values in symbol")

        if agg["timestamp_nulls"] > 0:
            errors.append("Null values in timestamp")

        if agg["negative_volume"] > 0:
            errors.append("Negative volume detected")

        return {
            "is_valid": len(errors) == 0,
            "errors": errors
        }