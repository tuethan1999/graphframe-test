from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, explode, lit, struct


def melt(
    df: DataFrame,
    id_vars: List[str],
    value_vars: List[str],
    var_name: str = "variable",
    value_name: str = "value",
    dropna: bool = False,
) -> DataFrame:
    """
    Transform a DataFrame from wide to long format by melting columns into rows.

    Args:
        df (DataFrame): The input DataFrame to be melted.
        id_vars (List[str]): The column names to use as identifier variables.
        value_vars (List[str]): The column names to unpivot into rows.
        var_name (str, optional): The name of the variable column. Defaults to "variable".
        value_name (str, optional): The name of the value column. Defaults to "value".
        dropna (bool, optional): Whether to drop rows with null values. Defaults to False.

    Returns:
        DataFrame: The melted DataFrame.
    """
    # Create an array of column names for the id variables
    id_cols = [col(c) for c in id_vars]

    # Create an array of column names for the value variables
    value_cols = [
        struct(lit(c).alias(var_name), col(c).alias(value_name)) for c in value_vars
    ]

    # Explode the value columns into rows
    exploded_df = df.select(id_cols + [explode(array(*value_cols)).alias("tmp")])

    # Extract the variable and value columns from the struct
    result_df = exploded_df.select(
        id_cols
        + [
            col("tmp")[var_name].alias(var_name),
            col("tmp")[value_name].alias(value_name),
        ]
    )

    # Drop null values if dropna is True
    if dropna:
        result_df = result_df.dropna()

    return result_df
