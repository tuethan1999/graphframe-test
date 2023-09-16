from receiptprocessor.graphframes.connected_components import create_graph_frame, ConnectedComponents
from pyspark.ml.param import Param

def test_ConnectedComponents(spark_session, cleaned_data, component_vertice_mapping):
    edge_columns = ["id", "Cashback"]
    vertice_columns = ["anid", "AdId", "SapphireId", "userIDFA"]
    connected_components_transformer = ConnectedComponents(EdgeColumns=edge_columns, VerticeColumns=vertice_columns)
    connected_components = connected_components_transformer.transform(cleaned_data)
    assert connected_components.collect() == component_vertice_mapping.collect()