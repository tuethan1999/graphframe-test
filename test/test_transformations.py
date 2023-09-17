from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from receiptprocessor.transformations import melt


def test_melt(spark_session: SparkSession):
    # Define the input DataFrame
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("income", IntegerType(), True),
            StructField("expenses", IntegerType(), True),
        ]
    )
    data = [
        (1, "Alice", 25, "F", 50000, 40000),
        (2, "Bob", 30, "M", 60000, 45000),
        (3, "Charlie", 35, "M", 70000, 50000),
    ]
    input_df = spark_session.createDataFrame(data, schema)

    # Define the expected output DataFrame
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("variable", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    expected_data = [
        (1, "name", "Alice"),
        (1, "age", 25),
        (1, "gender", "F"),
        (1, "income", 50000),
        (1, "expenses", 40000),
        (2, "name", "Bob"),
        (2, "age", 30),
        (2, "gender", "M"),
        (2, "income", 60000),
        (2, "expenses", 45000),
        (3, "name", "Charlie"),
        (3, "age", 35),
        (3, "gender", "M"),
        (3, "income", 70000),
        (3, "expenses", 50000),
    ]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    # Test the melt function
    actual_df = melt(input_df, ["id"], ["name", "age", "gender", "income", "expenses"])
    assert actual_df.collect() == expected_df.collect()

    # Test the melt function with dropna=True
    input_df_with_null = input_df.withColumn("income", lit(None))
    expected_df_with_null = expected_df.filter("variable != 'income'")
    actual_df_with_null = melt(
        input_df_with_null,
        ["id"],
        ["name", "age", "gender", "income", "expenses"],
        dropna=True,
    )
    assert actual_df_with_null.collect() == expected_df_with_null.collect()
