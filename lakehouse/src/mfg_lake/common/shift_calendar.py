"""Shared shift-calendar construction for migrated jobs."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from mfg_lake.common.timeconv import ts_seconds
from mfg_lake.common.tz import require_mapped, windows_to_iana_col


def time_of_day_seconds(df: DataFrame, col: str) -> Column:
    """TIME column -> seconds since midnight. CSV inferSchema reads 'HH:MM'
    as a timestamp on today's date; tests may pass plain strings."""
    if isinstance(df.schema[col].dataType, T.TimestampType):
        c = F.col(col)
        return F.hour(c) * 3600 + F.minute(c) * 60 + F.second(c)
    parts = F.split(F.col(col).cast("string"), ":")
    return (parts.getItem(0).cast("int") * 3600 + parts.getItem(1).cast("int") * 60
            + F.coalesce(parts.getItem(2).cast("int"), F.lit(0)))


def plants_with_iana(plant: DataFrame) -> DataFrame:
    """dim.plant with trimmed CHAR(5) plant_id and IANA tz; fails on unmapped tz."""
    require_mapped(r.tz_name for r in plant.select("tz_name").distinct().collect())
    return plant.select(F.trim("plant_id").alias("plant_id"),
                        windows_to_iana_col(F.col("tz_name")).alias("iana"))


def build_shift_calendar(plants: DataFrame, shift_pattern: DataFrame,
                         calendar: DataFrame) -> DataFrame:
    """dim.usp_refresh_shift_calendar -> dim.shift_calendar (UTC bounds).

    Local wall-clock bounds -> UTC via to_utc_timestamp (java.time: ambiguous
    fall-back times take the pre-transition offset, gap times shift forward),
    matching SQL Server AT TIME ZONE.
    """
    sp = shift_pattern.select(
        F.trim("plant_id").alias("plant_id"),
        F.trim("shift_code").alias("shift_code"),
        time_of_day_seconds(shift_pattern, "local_start").alias("start_sec"),
        time_of_day_seconds(shift_pattern, "local_end").alias("end_sec"),
        F.col("end_next_day").cast("int").alias("end_next_day"))
    cal = calendar.select(F.to_date("calendar_date").alias("production_day"))
    day_epoch = F.unix_timestamp(F.col("production_day").cast("timestamp"))
    start_local = F.timestamp_seconds(day_epoch + F.col("start_sec"))
    end_local = F.timestamp_seconds(day_epoch + F.col("end_next_day") * 86400
                                    + F.col("end_sec"))
    return (plants.join(sp, "plant_id").crossJoin(cal)
            .select("plant_id", "shift_code", "production_day",
                    ts_seconds(F.to_utc_timestamp(start_local, F.col("iana"))).alias("start_utc"),
                    ts_seconds(F.to_utc_timestamp(end_local, F.col("iana"))).alias("end_utc")))
