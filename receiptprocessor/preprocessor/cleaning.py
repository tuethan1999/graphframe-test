from typing import Optional

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import Param, TypeConverters
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, regexp_replace, when


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
            column_name,
            when(lower(col(column_name)).isin(null_string_list), None).otherwise(
                col(column_name)
            ),
        )
    return df


class DataCleaner(Transformer):
    @keyword_only
    def __init__(self, columns_to_clean: Optional[list[str]] = None):
        super(DataCleaner, self).__init__()
        self.columns_to_clean = Param(
            self,
            "columns_to_clean",
            "list of columns to clean",
            typeConverter=TypeConverters.toListString,
        )
        self.set(self.columns_to_clean,
                 columns_to_clean if columns_to_clean is not None else [])

    def _transform(self, df: DataFrame) -> DataFrame:
        self._verify_params()
        df = replace_zeros(df, self.getOrDefault("columns_to_clean"))
        df = replace_null_string_values(df, self.getOrDefault("columns_to_clean"))
        return df

    def _verify_params(self):
        columns_to_clean = self.getOrDefault("columns_to_clean")
        if columns_to_clean is None or len(columns_to_clean) == 0:
            raise ValueError("columns_to_clean cannot be empty")
