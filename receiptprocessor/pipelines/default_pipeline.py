from pyspark.ml import Pipeline

from receiptprocessor.graphframes.connected_components import ConnectedComponents
from receiptprocessor.preprocessor.cleaning import DataCleaner


class DefaultPipeline(Pipeline):
    def __init__(self):
        super().__init__()
        vertice_columns = ["anid", "AdId", "SapphireId", "userIDFA"]
        edge_columns = ["id", "Cashback"]
        dc = DataCleaner(columns_to_clean=vertice_columns)
        cc = ConnectedComponents(
            edge_columns=edge_columns, vertice_columns=vertice_columns
        )
        self.setStages([dc, cc])
