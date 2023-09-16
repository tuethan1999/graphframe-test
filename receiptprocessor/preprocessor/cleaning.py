from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, when, lower
from pyspark.ml import Transformer

def replace_zeros(df: DataFrame, column_names: list) -> DataFrame:
    non_alphanumeric_pattern = "[^a-zA-Z0-9]"
    for column_name in column_names:
        df = df.withColumn(
            column_name,
            when(
                regexp_replace(col(column_name), non_alphanumeric_pattern, "").cast(
                    "integer"
                )
                == 0,
                None,
            ).otherwise(col(column_name)),
        )
    return df

def replace_null_string_values(df: DataFrame, column_names: list) -> DataFrame:
    null_string_list = [
        "none",
        "null",
        "",
        "nan",
        "n/a",
        "na",
        "undefined",
        "unspecified",
    ]
    for column_name in column_names:
        df = df.withColumn(
            column_name, when(lower(col(column_name)).isin(null_string_list), None).otherwise(col(column_name))
        )
    return df

class DataCleaner(Transformer):
    def __init__(self, columns_to_clean: list):
        self.columns_to_clean = columns_to_clean

    def _transform(self, df: DataFrame) -> DataFrame:
        df = replace_zeros(df, self.columns_to_clean)
        df = replace_null_string_values(df, self.columns_to_clean)
        return df