"""PL_Line_Downtime -> curated line_downtime_daily (PySpark conversion).

Replaces the legacy chain (see legacy/adf/pipeline/PL_Line_Downtime.json):

  1. mes.usp_stg_downtime_local      -> stage_downtime_local()
  2. dim.shift_calendar              -> build_shift_calendar()
     mes.usp_split_downtime_by_shift -> split_by_shift()
  3. rpt.usp_rpt_line_downtime_daily -> rollup_daily()
     COPY rpt.line_downtime_daily    -> write_curated() (full overwrite)

Usage:
    python -m mfg_lake.jobs.line_downtime_daily --ns <ns> --as-of-utc <cutoff>

Mapping / semantics: docs/migration/line_downtime_daily.md
"""
import argparse
import logging
import re
from datetime import datetime, timedelta, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.spark import get_spark
from mfg_lake.common.tz import require_mapped, windows_to_iana_col

REPORT = "line_downtime_daily"
SOURCES = ["mes.downtime_event", "dim.line", "dim.plant", "dim.shift_pattern",
           "dim.calendar", "dim.downtime_reason"]
GRAIN = ["plant_id", "line_id", "production_day", "shift_code",
         "reason_category", "planned_flag"]
OUTPUT_COLUMNS = GRAIN + ["event_count", "downtime_minutes"]

log = logging.getLogger("mfg_lake.jobs.line_downtime_daily")

_AS_OF_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?\s*(Z|[+-]\d{2}:?\d{2})?$")


def parse_as_of_utc(value: str) -> str:
    """Normalise --as-of-utc to 'YYYY-MM-DD HH:MM:SS' (UTC, whole seconds).

    Accepts the Makefile form ('2025-11-17 00:00:00') and the ADF
    @trigger().scheduledTime form ('2025-11-17T06:30:00.0000000Z'). Fractional
    seconds round half-up, as SQL Server does converting a string to the
    proc's DATETIME2(0) @AsOfUtc parameter.
    """
    m = _AS_OF_RE.match(value.strip())
    if not m:
        raise ValueError(f"unparseable --as-of-utc {value!r}")
    day, hms, frac, off = m.groups()
    dt = datetime.strptime(f"{day} {hms}", "%Y-%m-%d %H:%M:%S")
    if frac and int(frac[0]) >= 5:
        dt += timedelta(seconds=1)
    if off and off != "Z":
        sign = 1 if off[0] == "+" else -1
        hh, mm = int(off[1:3]), int(off[-2:])
        dt = dt.replace(tzinfo=timezone(sign * timedelta(hours=hh, minutes=mm)))
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ts(col) -> Column:
    """Timestamp truncated to whole seconds (DATETIME2(0))."""
    c = F.col(col) if isinstance(col, str) else col
    return F.date_trunc("second", c.cast("timestamp"))


def _to_local(utc_col, iana_col) -> Column:
    """CAST(x AT TIME ZONE 'UTC' AT TIME ZONE tz AS DATETIME2(0))."""
    return F.date_trunc("second", F.from_utc_timestamp(utc_col, iana_col))


def datediff_minute(start, end) -> Column:
    """T-SQL DATEDIFF(MINUTE, start, end): minute boundaries crossed.

    floor(epoch(end)/60) - floor(epoch(start)/60) -- NOT elapsed seconds / 60.
    Operates on wall-clock values; relies on spark.sql.session.timeZone=UTC.
    """
    s = F.col(start) if isinstance(start, str) else start
    e = F.col(end) if isinstance(end, str) else end
    return (F.floor(F.unix_timestamp(e) / 60)
            - F.floor(F.unix_timestamp(s) / 60)).cast("int")


def _time_of_day_seconds(df: DataFrame, col: str) -> Column:
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


# ------------------------------------------------------------------ stage 1
def stage_downtime_local(event: DataFrame, line: DataFrame, plants: DataFrame,
                         as_of_utc: str) -> DataFrame:
    """mes.usp_stg_downtime_local -> stg.downtime_local (+ iana)."""
    as_of = F.lit(as_of_utc).cast("timestamp")
    e = event.select(
        F.col("event_id").cast("int").alias("event_id"),
        F.trim("line_id").alias("line_id"),
        F.trim("reason_code").alias("reason_code"),
        F.col("planned_flag").cast("int").cast("boolean").alias("planned_flag"),
        _ts("start_utc").alias("start_utc"),
        _ts("end_utc").alias("end_utc_raw"))
    ln = line.select(F.trim("line_id").alias("line_id"),
                     F.trim("plant_id").alias("plant_id"))
    return (e.join(ln, "line_id").join(plants, "plant_id")
             .where(F.col("start_utc") < as_of)
             .withColumn("end_utc", F.coalesce("end_utc_raw", as_of))
             .withColumn("start_local", _to_local(F.col("start_utc"), F.col("iana")))
             .withColumn("end_local", _to_local(F.col("end_utc"), F.col("iana")))
             .select("event_id", "plant_id", "line_id", "reason_code", "planned_flag",
                     "start_utc", "end_utc", "start_local", "end_local", "iana",
                     F.col("end_utc_raw").isNull().alias("capped_at_as_of")))


# ------------------------------------------------------------------ stage 2
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
        _time_of_day_seconds(shift_pattern, "local_start").alias("start_sec"),
        _time_of_day_seconds(shift_pattern, "local_end").alias("end_sec"),
        F.col("end_next_day").cast("int").alias("end_next_day"))
    cal = calendar.select(F.to_date("calendar_date").alias("production_day"))
    day_epoch = F.unix_timestamp(F.col("production_day").cast("timestamp"))
    start_local = F.timestamp_seconds(day_epoch + F.col("start_sec"))
    end_local = F.timestamp_seconds(day_epoch + F.col("end_next_day") * 86400
                                    + F.col("end_sec"))
    return (plants.join(sp, "plant_id").crossJoin(cal)
            .select("plant_id", "shift_code", "production_day",
                    _ts(F.to_utc_timestamp(start_local, F.col("iana"))).alias("start_utc"),
                    _ts(F.to_utc_timestamp(end_local, F.col("iana"))).alias("end_utc")))


def split_by_shift(local: DataFrame, shift_calendar: DataFrame) -> DataFrame:
    """mes.usp_split_downtime_by_shift -> stg.downtime_shift_seg.

    Overlap join done in UTC (sc.start_utc < e.end_utc AND sc.end_utc >
    e.start_utc); segment bounds clipped in UTC, then converted to plant
    local once. Equivalent to the legacy local-time join whenever shift
    boundaries don't fall inside a DST fold (see split_by_shift_local_legacy).
    """
    e, sc = local.alias("e"), shift_calendar.alias("sc")
    seg = (e.join(sc, (F.col("sc.plant_id") == F.col("e.plant_id"))
                  & (F.col("sc.start_utc") < F.col("e.end_utc"))
                  & (F.col("sc.end_utc") > F.col("e.start_utc")))
            .select("e.event_id", "e.plant_id", "e.line_id", "e.reason_code",
                    "e.planned_flag", "sc.production_day", "sc.shift_code",
                    F.greatest("e.start_utc", "sc.start_utc").alias("seg_start_utc"),
                    F.least("e.end_utc", "sc.end_utc").alias("seg_end_utc"),
                    "e.iana"))
    return (seg.withColumn("seg_start_local", _to_local(F.col("seg_start_utc"), F.col("iana")))
               .withColumn("seg_end_local", _to_local(F.col("seg_end_utc"), F.col("iana")))
               .drop("seg_start_utc", "seg_end_utc", "iana"))


def split_by_shift_local_legacy(local: DataFrame, shift_calendar: DataFrame) -> DataFrame:
    """Literal port of the legacy proc (join + clip on local wall-clock).
    Used only to verify equivalence with split_by_shift()."""
    sc = (shift_calendar.join(local.select("plant_id", "iana").distinct(), "plant_id")
          .withColumn("sc_start_local", _to_local(F.col("start_utc"), F.col("iana")))
          .withColumn("sc_end_local", _to_local(F.col("end_utc"), F.col("iana")))
          .drop("iana").alias("sc"))
    e = local.alias("e")
    return (e.join(sc, (F.col("sc.plant_id") == F.col("e.plant_id"))
                   & (F.col("sc.sc_start_local") < F.col("e.end_local"))
                   & (F.col("sc.sc_end_local") > F.col("e.start_local")))
             .select("e.event_id", "e.plant_id", "e.line_id", "e.reason_code",
                     "e.planned_flag", "sc.production_day", "sc.shift_code",
                     F.greatest("e.start_local", "sc.sc_start_local").alias("seg_start_local"),
                     F.least("e.end_local", "sc.sc_end_local").alias("seg_end_local")))


# ------------------------------------------------------------------ stage 3
def rollup_daily(seg: DataFrame, reason: DataFrame) -> DataFrame:
    """rpt.usp_rpt_line_downtime_daily (inner join to dim.downtime_reason)."""
    r = reason.select(F.trim("reason_code").alias("reason_code"),
                      F.col("reason_category").cast("string").alias("reason_category"))
    dropped = seg.join(r, "reason_code", "left_anti")
    n_dropped = dropped.count()
    if n_dropped:
        codes = sorted(x.reason_code for x in
                       dropped.select("reason_code").distinct().collect())
        log.warning("dropped %d segment(s) with reason_code not in dim.downtime_reason: %s",
                    n_dropped, codes)
    else:
        log.info("dropped 0 segments on dim.downtime_reason inner join")
    return (seg.join(r, "reason_code")
               .withColumn("minutes", datediff_minute("seg_start_local", "seg_end_local"))
               .groupBy(*GRAIN)
               .agg(F.countDistinct("event_id").cast("int").alias("event_count"),
                    F.sum("minutes").cast("int").alias("downtime_minutes"))
               .select(*OUTPUT_COLUMNS))


def build_report(frames: dict, as_of_utc: str) -> DataFrame:
    """frames: {schema.table: DataFrame} for SOURCES -> report DataFrame."""
    as_of_utc = parse_as_of_utc(as_of_utc)
    plants = plants_with_iana(frames["dim.plant"]).cache()

    local = stage_downtime_local(frames["mes.downtime_event"], frames["dim.line"],
                                 plants, as_of_utc).cache()
    n_src = frames["mes.downtime_event"].count()
    n_local = local.count()
    n_capped = local.where("capped_at_as_of").count()
    log.info("stage 1 stg.downtime_local: %d of %d events (start_utc < %s, line/plant "
             "matched); %d open event(s) capped at AsOfUtc",
             n_local, n_src, as_of_utc, n_capped)

    sc = build_shift_calendar(plants, frames["dim.shift_pattern"],
                              frames["dim.calendar"]).cache()
    seg = split_by_shift(local.drop("capped_at_as_of"), sc).cache()
    n_unshifted = local.join(seg.select("event_id").distinct(), "event_id",
                             "left_anti").count()
    log.info("stage 2 stg.downtime_shift_seg: %d segment(s) from %d shift-calendar rows; "
             "%d event(s) overlap no shift", seg.count(), sc.count(), n_unshifted)

    out = rollup_daily(seg, frames["dim.downtime_reason"])
    return out


def run(spark: SparkSession, ns: str, as_of_utc: str):
    frames = {t: read_raw(spark, t) for t in SOURCES}
    out = build_report(frames, as_of_utc).cache()
    n = out.count()
    dest = write_curated(out, REPORT, ns, mode="overwrite")
    log.info("stage 3 %s: wrote %d row(s) -> %s", REPORT, n, dest)
    return dest


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ns", required=True)
    ap.add_argument("--as-of-utc", required=True)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark(REPORT)
    try:
        run(spark, args.ns, args.as_of_utc)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
