"""Session timezone must be pinned to UTC: unix_timestamp()/string parsing
and any local-time logic depend on it. These fail if get_spark stops
setting spark.sql.session.timeZone=UTC."""
import pytest
from pyspark.sql import functions as F

from mfg_lake.common.spark import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test_tz")
    yield s
    s.stop()


def test_session_timezone_is_utc(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_unix_timestamp_parses_as_utc(spark):
    # epoch for '2025-10-20 08:00:00' interpreted in the session zone;
    # only UTC gives this exact value
    got = spark.sql(
        "SELECT unix_timestamp('2025-10-20 08:00:00') AS e").first().e
    assert got == 1760947200  # 2025-10-20T08:00:00Z


def test_from_utc_timestamp_value(spark):
    df = spark.createDataFrame([("2025-10-20 13:00:00",)], ["ts"])
    got = (df.select(F.from_utc_timestamp(
             F.col("ts").cast("timestamp"), F.lit("America/Chicago"))
             .alias("local")).first()["local"])
    assert str(got) == "2025-10-20 08:00:00"  # CDT, UTC-5
