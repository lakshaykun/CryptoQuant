from pyspark.sql import DataFrame, functions as F


def get_table_config(table_name: str, config: dict) -> dict:
    table_config = config["tables"].get(table_name)
    if not table_config:
        raise ValueError(f"Table config not found for '{table_name}'")
    return table_config


def add_metadata(df: DataFrame) -> DataFrame:
    df = df.withColumn("ingestion_time", F.current_timestamp())
    return df


def validate_partitions(df: DataFrame, partition_cols):
    if not partition_cols:
        return

    for col in partition_cols:
        if col not in df.columns:
            raise ValueError(f"Partition column '{col}' missing in DataFrame")