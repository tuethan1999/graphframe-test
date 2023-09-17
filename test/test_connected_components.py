from pyspark.sql import DataFrame
from pyspark.sql.functions import count

from receiptprocessor.graphframes.connected_components import ConnectedComponents


def test_ConnectedComponents(spark_session, cleaned_data, component_vertice_mapping):
    edge_columns = ["id", "Cashback"]
    vertice_columns = ["anid", "AdId", "SapphireId", "userIDFA"]
    connected_components_transformer = ConnectedComponents(
        edge_columns=edge_columns, vertice_columns=vertice_columns
    )
    connected_components = connected_components_transformer.transform(cleaned_data)
    assert are_component_counts_equal(connected_components, component_vertice_mapping)


def test_ConnectedComponents_set_parameter_in_transform(
    spark_session, cleaned_data, component_vertice_mapping
):
    edge_columns = ["id", "Cashback"]
    vertice_columns = ["anid", "AdId", "SapphireId", "userIDFA"]
    connected_components_transformer = ConnectedComponents()
    connected_components = connected_components_transformer.transform(
        cleaned_data,
        {
            connected_components_transformer.edge_columns: edge_columns,
            connected_components_transformer.vertice_columns: vertice_columns,
        },
    )
    assert are_component_counts_equal(connected_components, component_vertice_mapping)


def test_ConnectedComponents_setParameter(spark_session, cleaned_data):
    connected_components_transformer = ConnectedComponents()
    try:
        connected_components_transformer.transform(cleaned_data)
    except ValueError as e:
        assert str(e) == "EdgeColumns cannot be empty"


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
