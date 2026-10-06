"""IO helpers: read raw seed feeds, write curated parquet."""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .paths import curated_dir, raw_csv


def read_raw(spark: SparkSession, schema_table: str) -> DataFrame:
    """Read a mes.*/dim.* seed CSV (header row, inferred schema)."""
    path = raw_csv(schema_table)
    if not path.exists():
        raise FileNotFoundError(f"raw feed not found: {path} (run `make seed`)")
    return spark.read.option("header", True).option("inferSchema", True).csv(str(path))


def write_curated(df: DataFrame, report: str, ns: str, mode: str = "overwrite") -> Path:
    """Write a report dataframe to the resolved curated path as parquet."""
    dest = curated_dir(report, ns)
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.coalesce(1).write.mode(mode).parquet(str(dest))
    return dest
