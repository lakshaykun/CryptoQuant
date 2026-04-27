from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from models.inference.api_client import predict_with_api

class Log_Return_Lead1_Transformer:
    @staticmethod
    def transform(df: SparkDataFrame) -> SparkDataFrame:
        if df is None:
            return

        valid_rows = df.filter(F.col("is_valid_feature_row") == True)

        records_processed = valid_rows.count()

        if records_processed == 0:
            return

        pdf = valid_rows.toPandas()

        if pdf.empty:
            return

        pdf = pdf.copy()
        pdf["prediction"] = predict_with_api(pdf)

        result_df = valid_rows.sparkSession.createDataFrame(pdf).selectExpr(
            "cast(open_time as timestamp) as open_time",
            "cast(symbol as string) as symbol",
            "cast(date as date) as date",
            "cast(prediction as double) as prediction",
        )

        result_df = result_df.dropDuplicates(["symbol", "open_time"])

        return result_df