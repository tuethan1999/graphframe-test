import os
from typing import Optional, List

from graphframes import GraphFrame
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import Param, TypeConverters
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from receiptprocessor.transformations import melt
from receiptprocessor.utils.disk import write_parquet_safe


def create_graph_frame(
    df: DataFrame, edge_cols, vertice_cols, src_col="src", dst_col="dst"
) -> GraphFrame:
    melted_df = melt(
        df, edge_cols, vertice_cols, var_name="Type", value_name="value", dropna=True
    )
    src = melted_df.select(*edge_cols, col("value").alias(src_col)).alias("src")
    dst = melted_df.select(*edge_cols, col("value").alias(dst_col)).alias("dst")
    edges = (
        src.join(dst, on=edge_cols)
        .where(col(src_col) != col(dst_col))
        .select(src_col, dst_col, *edge_cols)
    ).repartition("src", "dst")
    vertices = melted_df.select(col("value").alias("id"), "Type").distinct()
    g = GraphFrame(vertices, edges)
    return g


class ConnectedComponents(Transformer):
    @keyword_only
    def __init__(
        self,
        edge_columns: Optional[List[str]] = None,
        vertice_columns: Optional[List[str]] = None,
        save_graphframe_path: Optional[str] = None,
    ):
        super(ConnectedComponents, self).__init__()
        self.edge_columns = Param(
            self,
            "edge_columns",
            "list of columns that represent edges",
            typeConverter=TypeConverters.toListString,
        )
        self.vertice_columns = Param(
            self,
            "vertice_columns",
            "list of columns that represent vertices",
            typeConverter=TypeConverters.toListString,
        )
        self.save_graphframe_path = Param(
            self,
            "save_graphframe_path",
            "path to save vertice and edge dfs to",
            typeConverter=TypeConverters.identity,
        )
        self.set(self.edge_columns, edge_columns if edge_columns is not None else [])
        self.set(
            self.vertice_columns, vertice_columns if vertice_columns is not None else []
        )
        self.set(self.save_graphframe_path, save_graphframe_path)

    def _transform(self, df: DataFrame) -> DataFrame:
        self._verify_params()
        g = create_graph_frame(
            df, self.getOrDefault("edge_columns"), self.getOrDefault("vertice_columns")
        )
        self._save_graphframe(g)
        cc = g.connectedComponents()
        return cc

    def _verify_params(self):
        edge_columns = self.getOrDefault("edge_columns")
        vertice_columns = self.getOrDefault("vertice_columns")
        if edge_columns is None or len(edge_columns) == 0:
            raise ValueError("EdgeColumns cannot be empty")
        if vertice_columns is None or len(vertice_columns) == 0:
            raise ValueError("VerticeColumns cannot be empty")

    def _save_graphframe(self, g: GraphFrame):
        save_graphframe_path = self.getOrDefault("save_graphframe_path")
        if save_graphframe_path is not None:
            write_parquet_safe(
                g.vertices, os.path.join(save_graphframe_path, "vertices")
            )
            write_parquet_safe(g.edges, os.path.join(save_graphframe_path, "edges"))
