from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
from graphframes import GraphFrame
from receiptprocessor.transformations import melt
from pyspark.ml.param.shared import Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark import keyword_only

from pyspark.ml import Transformer

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
    def __init__(self, EdgeColumns=None, VerticeColumns=None):
        super(ConnectedComponents, self).__init__()
        self.EdgeColumns = Param(self, "EdgeColumns", "")
        self.VerticeColumns = Param(self, "VerticeColumns", "")
        self._setDefault(EdgeColumns=[], VerticeColumns=[])
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:
        g = create_graph_frame(df, self.getEdgeColumns(), self.getVerticeColumns())
        cc = g.connectedComponents()
        return cc
    
    def setParams(self, EdgeColumns=None, VerticeColumns=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)
    
    def getEdgeColumns(self):
        return self.getOrDefault(self.EdgeColumns)
    
    def setEdgeColumns(self, value):
        return self._set(EdgeColumns=value)
    
    def getVerticeColumns(self):
        return self.getOrDefault(self.VerticeColumns)
    
    def setVerticeColumns(self, value):
        return self._set(VerticeColumns=value)