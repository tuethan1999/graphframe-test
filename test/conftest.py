import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("local-tests")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12")
        .config("spark.sql.broadcastTimeout", 2000)
        .getOrCreate()
    )
    yield spark

@pytest.fixture()
def uncleaned_data(spark_session):
    df = spark_session.createDataFrame(
        [
            ("id1", "anid1", "", "userIDFA1", "SapphireId1", 0.1),
            ("id2", "anid1", "AdId1", None, None, 0.2),
            ("id3", "anid2", "AdId1", None, "SapphireId2", 0.3),
            ("id4", "anid3", None, None, "SapphireId2", 0.4),
            ("id5", "anid4", None, "userIDFA2", None, 0.5),
            ("id6", "anid5", "", "userIDFA2", "SapphireId3", 0.6),
            ("id7", "anid5", None, "userIDFA2", "SapphireId3", 0.7),
            ("id8", "anid5", "0000-0", "userIDFA2", "SapphireId3", 0.8),
            ("id9", "anid6", "0000-0", None, None, 0.9),
        ],
        ["id", "anid", "AdId", "userIDFA", "SapphireId", "Cashback"],
    )
    yield df