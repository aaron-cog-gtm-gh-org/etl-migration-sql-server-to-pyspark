"""PL_Line_Downtime -> curated line_downtime_daily.

Replaces, in one Spark job:
  mes.usp_stg_downtime_local      (stg.downtime_local)
  mes.usp_split_downtime_by_shift (stg.downtime_shift_seg)
  rpt.usp_rpt_line_downtime_daily (rpt.line_downtime_daily)

    python -m mfg_lake.jobs.line_downtime_daily --ns <ns> --as-of-utc <cutoff>

--as-of-utc is the legacy AsOfUtc proc parameter: events starting at or
after it are excluded and open events (end_utc NULL) are capped at it.
The shift-calendar window is the full dim.calendar.
"""
import argparse
import re
import sys
from datetime import datetime, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.paths import abfss_uri
from mfg_lake.common.spark import get_spark
from mfg_lake.jobs.daily_production import build_shift_calendar, plant_zones

REPORT = "line_downtime_daily"
FEEDS = ("mes.downtime_event", "dim.line", "dim.plant", "dim.downtime_reason",
         "dim.shift_pattern", "dim.calendar")
# mes.downtime_event NOT NULL columns (legacy/sql/schema/mes.sql)
NOT_NULL = ("event_id", "line_id", "start_utc", "reason_code", "planned_flag")
OUTPUT_COLUMNS = ("plant_id", "line_id", "production_day", "shift_code",
                  "reason_category", "planned_flag", "event_count",
                  "downtime_minutes")
STG_DOWNTIME_LOCAL_COLUMNS = ("event_id", "plant_id", "line_id", "reason_code",
                              "planned_flag", "start_utc", "end_utc",
                              "start_local", "end_local")
STG_DOWNTIME_SHIFT_SEG_COLUMNS = ("event_id", "plant_id", "line_id",
                                  "reason_code", "planned_flag",
                                  "production_day", "shift_code",
                                  "seg_start_local", "seg_end_local")
INT_MIN, INT_MAX = -(2**31), 2**31 - 1


_AS_OF_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?"
    r"(Z|[+-]\d{2}:\d{2})?$")


def parse_as_of_utc(value) -> datetime:
    """@AsOfUtc text -> naive UTC datetime at whole-second precision.

    Accepts 'YYYY-MM-DD HH:MM:SS' or ISO 'T', optional fractional seconds
    (rounded half-up, as a string -> DATETIME2(0) conversion) and an
    optional 'Z' or '+hh:mm' offset (converted to UTC). A datetime is
    returned unchanged.
    """
    if isinstance(value, datetime):
        return value
    m = _AS_OF_RE.fullmatch(str(value).strip())
    if not m:
        raise ValueError(f"not a 'YYYY-MM-DD HH:MM:SS' datetime: {value!r}")
    dt = datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}")
    frac = m.group(3)
    if frac and int((frac + "000000")[:6]) >= 500000:
        dt += timedelta(seconds=1)
    off = m.group(4)
    if off and off != "Z":
        sign = 1 if off[0] == "+" else -1
        dt -= sign * timedelta(hours=int(off[1:3]), minutes=int(off[4:6]))
    return dt


def datediff_minute(start, end):
    """T-SQL DATEDIFF(MINUTE, start, end): minute boundaries crossed.

    floor(epoch(end)/60) - floor(epoch(start)/60); correct for
    end < start (negative result), unlike elapsed-seconds division.
    """
    def _c(c):
        return c if isinstance(c, Column) else F.col(c)
    return (F.floor(_c(end).cast("long") / 60)
            - F.floor(_c(start).cast("long") / 60)).cast("int")


def _as_of_lit(as_of_utc):
    # string -> timestamp is interpreted in the session zone (UTC), unlike
    # F.lit(datetime) which converts through the Python process's zone
    return F.lit(parse_as_of_utc(as_of_utc).strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")


def _zones(plant: DataFrame) -> DataFrame:
    return plant_zones(plant.withColumn("plant_id", F.trim("plant_id")))


def stage_downtime_local(downtime_event: DataFrame, line: DataFrame,
                         plant: DataFrame, as_of_utc) -> DataFrame:
    """mes.usp_stg_downtime_local -> stg.downtime_local.

    NOT NULL drops, inner joins to dim.line/dim.plant, start_utc < AsOfUtc,
    end_utc capped by ISNULL(end_utc, AsOfUtc), UTC -> plant-local via the
    per-plant zone (AT TIME ZONE).
    """
    as_of = _as_of_lit(as_of_utc)
    e = downtime_event.select(
        F.col("event_id").cast("int").alias("event_id"),
        F.trim(F.col("line_id").cast("string")).alias("line_id"),
        F.col("start_utc").cast("timestamp").alias("start_utc"),
        F.col("end_utc").cast("timestamp").alias("end_utc"),
        F.trim(F.col("reason_code").cast("string")).alias("reason_code"),
        (F.col("planned_flag").cast("int") != 0).alias("planned_flag"),
    ).dropna(subset=list(NOT_NULL))
    lines = line.select(F.trim(F.col("line_id").cast("string")).alias("line_id"),
                        F.trim(F.col("plant_id").cast("string")).alias("plant_id"))
    end_utc = F.coalesce(F.col("end_utc"), as_of)
    return (e
            .join(lines, "line_id")
            .join(_zones(plant), "plant_id")
            .where(F.col("start_utc") < as_of)
            .select(
                "event_id", "plant_id", "line_id", "reason_code", "planned_flag",
                "start_utc", end_utc.alias("end_utc"),
                F.from_utc_timestamp("start_utc", F.col("tz")).alias("start_local"),
                F.from_utc_timestamp(end_utc, F.col("tz")).alias("end_local")))


def split_downtime_by_shift(downtime_local: DataFrame, plant: DataFrame,
                            shift_calendar: DataFrame) -> DataFrame:
    """mes.usp_split_downtime_by_shift -> stg.downtime_shift_seg.

    Shift bounds are converted UTC -> local (the proc converts
    shift_calendar's UTC bounds back via AT TIME ZONE, so the repeated
    fall-back hour is wall-clock-ambiguous on the event side too — a local
    span ending before it starts still joins). Overlap is strict: a
    zero-length event on a boundary touches neither shift.
    """
    sc = (shift_calendar
          .withColumn("plant_id", F.trim("plant_id"))
          .join(_zones(plant), "plant_id")
          .select(
              "plant_id", "shift_code", "production_day",
              F.from_utc_timestamp("start_utc", F.col("tz")).alias("sc_start_local"),
              F.from_utc_timestamp("end_utc", F.col("tz")).alias("sc_end_local")))
    e = downtime_local.alias("e")
    return (e.join(sc.alias("sc"),
                   (F.col("sc.plant_id") == F.col("e.plant_id"))
                   & (F.col("sc.sc_start_local") < F.col("e.end_local"))
                   & (F.col("sc.sc_end_local") > F.col("e.start_local")))
            .select(
                "e.event_id", "e.plant_id", "e.line_id", "e.reason_code",
                "e.planned_flag", "sc.production_day", "sc.shift_code",
                F.greatest("e.start_local", "sc.sc_start_local").alias("seg_start_local"),
                F.least("e.end_local", "sc.sc_end_local").alias("seg_end_local")))


def build_report(downtime_shift_seg: DataFrame,
                 downtime_reason: DataFrame) -> DataFrame:
    """rpt.usp_rpt_line_downtime_daily.

    Grain is (plant, line, production_day, shift, reason_category,
    planned_flag); event_count is COUNT(DISTINCT event_id) so duplicate
    staging rows still count once while their minutes double.
    """
    r = downtime_reason.select(
        F.trim(F.col("reason_code").cast("string")).alias("reason_code"),
        F.trim(F.col("reason_category").cast("string")).alias("reason_category"))
    g = (downtime_shift_seg.join(r, "reason_code")
         .groupBy("plant_id", "line_id", "production_day", "shift_code",
                  "reason_category", "planned_flag")
         .agg(F.countDistinct("event_id").alias("event_count"),
              F.sum(datediff_minute("seg_start_local", "seg_end_local"))
              .alias("_minutes_raw")))
    return g.select(
        F.col("plant_id").cast("string").alias("plant_id"),
        F.col("line_id").cast("string").alias("line_id"),
        F.col("production_day").cast("date").alias("production_day"),
        F.col("shift_code").cast("string").alias("shift_code"),
        F.col("reason_category").cast("string").alias("reason_category"),
        F.col("planned_flag").cast("boolean").alias("planned_flag"),
        F.col("event_count").cast("int").alias("event_count"),
        F.col("_minutes_raw").cast("int").alias("downtime_minutes"),
        F.col("_minutes_raw"))


def _check_int_overflow(df: DataFrame) -> None:
    """SQL Server SUM(INT) raises on overflow; Spark's int cast would wrap."""
    bad = df.filter((F.col("_minutes_raw") > INT_MAX)
                    | (F.col("_minutes_raw") < INT_MIN)).count()
    if bad:
        raise ArithmeticError(f"{bad} groups overflow INT (Arithmetic overflow in legacy)")


def transform(feeds: dict, as_of_utc) -> DataFrame:
    """Raw feeds (keyed by schema.table) -> rpt.line_downtime_daily rows."""
    sc = build_shift_calendar(feeds["dim.plant"], feeds["dim.shift_pattern"],
                              feeds["dim.calendar"])
    stg = stage_downtime_local(feeds["mes.downtime_event"], feeds["dim.line"],
                               feeds["dim.plant"], as_of_utc)
    seg = split_downtime_by_shift(stg, feeds["dim.plant"], sc)
    return build_report(seg, feeds["dim.downtime_reason"]).select(*OUTPUT_COLUMNS)


def run(ns: str, as_of_utc: str, spark: SparkSession = None):
    spark = spark or get_spark(f"mfg_lake.{REPORT}")
    feeds = {t: read_raw(spark, t) for t in FEEDS}
    ev = feeds["mes.downtime_event"]
    n_in = ev.count()
    clean = ev.dropna(subset=list(NOT_NULL))
    lines = feeds["dim.line"].select(
        F.trim("line_id").alias("line_id"),
        F.trim("plant_id").alias("plant_id")).distinct()
    n_joined = (clean.join(lines, "line_id")
                .join(_zones(feeds["dim.plant"]), "plant_id").count())
    stg = stage_downtime_local(ev, feeds["dim.line"], feeds["dim.plant"],
                               as_of_utc).cache()
    sc = build_shift_calendar(feeds["dim.plant"], feeds["dim.shift_pattern"],
                              feeds["dim.calendar"])
    seg = split_downtime_by_shift(stg, feeds["dim.plant"], sc).cache()
    rpt = build_report(seg, feeds["dim.downtime_reason"]).cache()
    _check_int_overflow(rpt)

    n_stg = stg.count()
    open_capped = (clean.filter(F.col("end_utc").isNull())
                   .join(stg.select("event_id"), "event_id").count())
    no_shift = (stg.join(seg.select("event_id").distinct(), "event_id", "left_anti")
                .count())
    reasons = feeds["dim.downtime_reason"].select(
        F.trim("reason_code").alias("reason_code"))
    seg_bad_reason = seg.join(reasons, "reason_code", "left_anti").count()

    out = (rpt.select(*OUTPUT_COLUMNS)
           .orderBy("plant_id", "line_id", "production_day", "shift_code",
                    "reason_category", "planned_flag"))
    dest = write_curated(out, REPORT, ns)
    print(f"[{REPORT}] as_of_utc={as_of_utc} events={n_in} "
          f"dropped_not_null={n_in - clean.count()} "
          f"dropped_unknown_line_or_plant={clean.count() - n_joined} "
          f"dropped_start_at_or_after_as_of={n_joined - n_stg} "
          f"open_events_capped={open_capped} staged={n_stg} "
          f"events_without_shift={no_shift} segments={seg.count()} "
          f"segments_dropped_unknown_reason={seg_bad_reason} "
          f"rows={rpt.count()}")
    print(f"[{REPORT}] wrote {abfss_uri(REPORT)} -> {dest}")
    stg.unpersist()
    seg.unpersist()
    rpt.unpersist()
    return dest


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ns", required=True, help="lake namespace (local: out/<ns>)")
    ap.add_argument("--as-of-utc", required=True,
                    help="report cutoff 'YYYY-MM-DD HH:MM:SS' (AsOfUtc)")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    spark = get_spark(f"mfg_lake.{REPORT}")
    try:
        run(args.ns, args.as_of_utc, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
