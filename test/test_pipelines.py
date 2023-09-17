from pyspark.sql import DataFrame
from utils import are_component_counts_equal

from receiptprocessor.pipelines.default_pipeline import DefaultPipeline


def test_default_pipeline(
    uncleaned_data: DataFrame, component_vertice_mapping: DataFrame
):
    default_pipeline = DefaultPipeline()
    default_pipeline_model = default_pipeline.fit(uncleaned_data)
    result = default_pipeline_model.transform(uncleaned_data)
    assert are_component_counts_equal(result, component_vertice_mapping)
