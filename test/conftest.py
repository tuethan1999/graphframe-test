import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("local-tests")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12")
        .config(
            "spark.sql.broadcastTimeout", 2000
        )  # test runs faster with these configs
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedJobs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir(".spark_checkpoint")
    yield spark


@pytest.fixture()
def uncleaned_data(spark_session):
    data = [
        ("id1", "anid1", "", "userIDFA1", "SapphireId1", 0.1),
        ("id2", "anid1", "AdId1", None, None, 0.2),
        ("id3", "anid2", "AdId1", None, "SapphireId2", 0.3),
        ("id4", "anid3", None, None, "SapphireId2", 0.4),
        ("id5", "anid4", None, "userIDFA2", None, 0.5),
        ("id6", "anid5", "", "userIDFA2", "SapphireId3", 0.6),
        ("id7", "anid5", None, "userIDFA2", "SapphireId3", 0.7),
        ("id8", "anid5", "0000-0", "userIDFA2", "SapphireId3", 0.8),
        ("id9", "anid6", "0000-0", None, None, 0.9),
    ]
    df = spark_session.createDataFrame(
        data,
        ["id", "anid", "AdId", "userIDFA", "SapphireId", "Cashback"],
    )
    yield df


@pytest.fixture()
def cleaned_data(spark_session):
    data = [
        ("id1", "anid1", None, "userIDFA1", "SapphireId1", 0.1),
        ("id2", "anid1", "AdId1", None, None, 0.2),
        ("id3", "anid2", "AdId1", None, "SapphireId2", 0.3),
        ("id4", "anid3", None, None, "SapphireId2", 0.4),
        ("id5", "anid4", None, "userIDFA2", None, 0.5),
        ("id6", "anid5", None, "userIDFA2", "SapphireId3", 0.6),
        ("id7", "anid5", None, "userIDFA2", "SapphireId3", 0.7),
        ("id8", "anid5", None, "userIDFA2", "SapphireId3", 0.8),
        ("id9", "anid6", None, None, None, 0.9),
    ]

    df = spark_session.createDataFrame(
        data, ["id", "anid", "AdId", "userIDFA", "SapphireId", "Cashback"]
    )
    yield df


@pytest.fixture()
def component_vertice_mapping(spark_session):
    data = [
        ("AdId1", "AdId", 369367187456),
        ("anid3", "anid", 369367187456),
        ("anid1", "anid", 369367187456),
        ("SapphireId1", "SapphireId", 369367187456),
        ("anid2", "anid", 369367187456),
        ("userIDFA1", "userIDFA", 369367187456),
        ("SapphireId2", "SapphireId", 369367187456),
        ("userIDFA2", "userIDFA", 8589934592),
        ("SapphireId3", "SapphireId", 8589934592),
        ("anid6", "anid", 111669149696),
        ("anid4", "anid", 8589934592),
        ("anid5", "anid", 8589934592),
    ]
    df = spark_session.createDataFrame(data, ["id", "Type", "component"])
    yield df
