"""Local Spark session builder for mfg_lake jobs."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "mfg_lake") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
