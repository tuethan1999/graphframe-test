from pyspark.sql import DataFrame
from pyspark.sql.functions import count


def are_component_counts_equal(df: DataFrame, expected_df: DataFrame) -> bool:
    component_count_column = "component_count"
    actual_component_counts = (
        df.groupBy("component")
        .agg(count("*").alias(component_count_column))
        .select(component_count_column)
        .orderBy(component_count_column)
    )
    expected_component_counts = (
        expected_df.groupBy("component")
        .agg(count("*").alias(component_count_column))
        .select(component_count_column)
        .orderBy(component_count_column)
    )
    return actual_component_counts.collect() == expected_component_counts.collect()
