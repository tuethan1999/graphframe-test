from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from receiptprocessor.preprocessor.cleaning import (
    DataCleaner,
    replace_null_string_values,
    replace_zeros,
)


def test_replace_zeros(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("anid", StringType(), True),
            StructField("AdId", StringType(), True),
        ]
    )
    data = [("0", "0-000", "0#@#$000"), ("1", "0", "123"), ("2", "null", "None")]
    df = spark_session.createDataFrame(data, schema)
    cleaned_df = replace_zeros(df, ["anid", "AdId"])
    expected_data = [("0", None, None), ("1", None, "123"), ("2", "null", "None")]
    expected_df = spark_session.createDataFrame(expected_data, schema)
    assert cleaned_df.collect() == expected_df.collect()


def test_replace_null_string_values(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("anid", StringType(), True),
            StructField("AdId", StringType(), True),
        ]
    )
    data = [
        ("0", None, "0000-0"),
        ("1", "null", None),
        ("2", "0000-0", "NA"),
        ("3", "NaN", ""),
        ("4", "Unspecified", "undefined"),
    ]
    df = spark_session.createDataFrame(data, schema)
    expected_data = [
        ("0", None, "0000-0"),
        ("1", None, None),
        ("2", "0000-0", None),
        ("3", None, None),
        ("4", None, None),
    ]
    expected_df = spark_session.createDataFrame(expected_data, schema)
    cleaned_df = replace_null_string_values(df, ["anid", "AdId"])
    assert cleaned_df.collect() == expected_df.collect()


def test_cleaning_transformer2(
    spark_session: SparkSession, uncleaned_data: DataFrame, cleaned_data: DataFrame
):
    cleaner = DataCleaner(["anid", "AdId", "SapphireId", "userIDFA"])
    cleaned_df = cleaner.transform(uncleaned_data)
    assert cleaned_df.collect() == cleaned_data.collect()
