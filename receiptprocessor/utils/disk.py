from pyspark.sql import DataFrame
import uuid
import os

def write_parquet_safe(df: DataFrame, path: str, verbose: bool = True) -> None:
    random_string = str(uuid.uuid4())
    final_path = os.path.join(path, "{}-{}".format(df.name, random_string))
    df.write.parquet(final_path)
    if verbose:
        print("Wrote {} to: {}".format(df.name, final_path))