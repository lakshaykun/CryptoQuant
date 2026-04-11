from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

def validate_schema(df: DataFrame, expected_schema: StructType):
    df_schema = {f.name: f.dataType for f in df.schema.fields}
    expected = {f.name: f.dataType for f in expected_schema.fields}

    if df_schema != expected:
        raise ValueError(f"Schema mismatch:\nExpected: {expected}\nGot: {df_schema}")
    
    return True