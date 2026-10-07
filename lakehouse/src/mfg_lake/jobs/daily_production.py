"""PL_Daily_Production -> curated daily_production.

Replaces, in one Spark job:
  dim.usp_refresh_shift_calendar  (shift calendar, built in-job; no feed)
  mes.usp_stg_production_counts   (stg.production_local)
  rpt.usp_rpt_daily_production    (rpt.daily_production)

    python -m mfg_lake.jobs.daily_production --ns <ns> --as-of-utc <cutoff>

--as-of-utc is accepted for the job convention but, like the legacy
pipeline (which declares AsOfUtc and never passes it to the procs), does
not filter anything. The shift-calendar window is the full dim.calendar.
"""
import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.paths import abfss_uri
from mfg_lake.common.spark import get_spark
from mfg_lake.common.tz import windows_to_iana

REPORT = "daily_production"
FEEDS = ("mes.production_count", "dim.line", "dim.plant", "dim.sku",
         "dim.shift_pattern", "dim.calendar")
# mes.production_count NOT NULL columns (legacy/sql/schema/mes.sql)
NOT_NULL = ("line_id", "bucket_start_utc", "bucket_end_utc", "sku_id",
            "total_units", "good_units")
OUTPUT_COLUMNS = ("plant_id", "line_id", "production_day", "sku_id",
                  "total_units", "good_units", "cases", "yield_pct")
INT_MAX = 2**31 - 1


def plant_zones(plant: DataFrame) -> DataFrame:
    """plant_id -> IANA zone id, mapped driver-side from dim.plant.tz_name."""
    rows = [(r.plant_id, windows_to_iana(r.tz_name))
            for r in plant.select("plant_id", "tz_name").distinct().collect()]
    return plant.sparkSession.createDataFrame(rows, "plant_id string, tz string")


def _time_of_day(df: DataFrame, col: str):
    """TIME column as 'HH:MM[:SS]' text. CSV inferSchema reads '06:00' as a
    timestamp on today's date, so keep only its time-of-day part."""
    if dict(df.dtypes)[col] == "timestamp":
        return F.date_format(F.col(col), "HH:mm:ss")
    return F.col(col).cast("string")


def _seconds_since_midnight(t):
    """'HH:MM' / 'HH:MM:SS' -> seconds (DATEDIFF(SECOND, '00:00', t))."""
    parts = F.split(t, ":")
    return (parts.getItem(0).cast("int") * 3600 + parts.getItem(1).cast("int") * 60
            + F.coalesce(parts.getItem(2).cast("int"), F.lit(0)))


def build_shift_calendar(plant: DataFrame, shift_pattern: DataFrame,
                         calendar: DataFrame) -> DataFrame:
    """dim.usp_refresh_shift_calendar over every dim.calendar date.

    Local shift bounds -> UTC via the plant zone. to_utc_timestamp resolves
    DST like AT TIME ZONE: a time in the spring-forward gap is moved forward
    by the gap, an ambiguous fall-back time takes the pre-transition (DST)
    offset. So the night shift spanning fall-back is one hour longer.
    """
    shift_pattern = shift_pattern.select(
        "plant_id", "shift_code", "end_next_day",
        _time_of_day(shift_pattern, "local_start").alias("local_start"),
        _time_of_day(shift_pattern, "local_end").alias("local_end"))
    day0 = F.col("calendar_date").cast("date").cast("timestamp").cast("long")
    local_start = F.timestamp_seconds(day0 + _seconds_since_midnight(F.col("local_start")))
    local_end = F.timestamp_seconds(day0 + F.col("end_next_day").cast("long") * 86400
                                    + _seconds_since_midnight(F.col("local_end")))
    return (plant.select("plant_id")
            .join(plant_zones(plant), "plant_id")
            .join(shift_pattern, "plant_id")
            .crossJoin(calendar.select("calendar_date"))
            .select(
                "plant_id", "shift_code",
                F.col("calendar_date").cast("date").alias("production_day"),
                F.to_utc_timestamp(local_start, F.col("tz")).alias("start_utc"),
                F.to_utc_timestamp(local_end, F.col("tz")).alias("end_utc")))


def stage_production_local(production_count: DataFrame, line: DataFrame,
                           plant: DataFrame, shift_calendar: DataFrame) -> DataFrame:
    """mes.usp_stg_production_counts -> stg.production_local."""
    pc = production_count.select(
        F.col("line_id").cast("string").alias("line_id"),
        F.col("sku_id").cast("string").alias("sku_id"),
        F.col("bucket_start_utc").cast("timestamp").alias("bucket_start_utc"),
        F.col("bucket_end_utc").cast("timestamp").alias("bucket_end_utc"),
        F.col("total_units").cast("int").alias("total_units"),
        F.col("good_units").cast("int").alias("good_units"),
    ).dropna(subset=list(NOT_NULL))
    sc = shift_calendar.select(
        F.col("plant_id").alias("sc_plant_id"), "shift_code",
        F.col("start_utc").alias("sc_start"), F.col("end_utc").alias("sc_end"))
    local = F.from_utc_timestamp(F.col("bucket_start_utc"), F.col("tz"))
    return (pc
            .join(line.select("line_id", "plant_id"), "line_id")
            .join(plant_zones(plant), "plant_id")
            .join(sc, (F.col("sc_plant_id") == F.col("plant_id"))
                  & (F.col("bucket_start_utc") >= F.col("sc_start"))
                  & (F.col("bucket_start_utc") < F.col("sc_end")))
            .select(
                "line_id", "plant_id", "sku_id",
                # dim.ufn_production_day: date(local - 6h), by bucket START
                F.to_date(local - F.expr("INTERVAL 6 HOURS")).alias("production_day"),
                "shift_code",
                ((F.col("bucket_end_utc").cast("long") - F.col("bucket_start_utc").cast("long"))
                 / 60).cast("int").alias("bucket_minutes"),
                "total_units", "good_units"))


def build_report(stg: DataFrame, sku: DataFrame) -> DataFrame:
    """rpt.usp_rpt_daily_production."""
    k = sku.select(F.col("sku_id").cast("string").alias("sku_id"),
                   F.col("pack_size").cast("int").alias("pack_size"))
    g = (stg.join(k, "sku_id")
         .groupBy("plant_id", "line_id", "production_day", "sku_id", "pack_size")
         .agg(F.sum("total_units").alias("total"), F.sum("good_units").alias("good")))
    # CAST(SUM(good) AS DECIMAL(18,4)) / NULLIF(SUM(total),0) * 100, ROUND(.., 2).
    # Decimal arithmetic + Spark round() (HALF_UP == T-SQL half away from zero).
    ratio = (F.col("good").cast("decimal(18,4)")
             / F.when(F.col("total") != 0, F.col("total")).cast("decimal(10,0)")
             * F.lit(100).cast("decimal(10,0)"))
    return g.select(
        F.col("plant_id").cast("string").alias("plant_id"),
        F.col("line_id").cast("string").alias("line_id"),
        F.col("production_day").cast("date").alias("production_day"),
        F.col("sku_id").cast("string").alias("sku_id"),
        F.col("total").cast("int").alias("total_units"),
        F.col("good").cast("int").alias("good_units"),
        # INT / INT in T-SQL truncates toward zero
        F.expr("good div pack_size").cast("int").alias("cases"),
        F.round(ratio, 2).cast("decimal(9,2)").alias("yield_pct"),
        F.col("total").alias("_total_raw"), F.col("good").alias("_good_raw"),
    )


def _check_int_overflow(df: DataFrame) -> None:
    """SQL Server SUM(INT) raises on overflow; Spark's int cast would wrap."""
    bad = df.filter((F.abs("_total_raw") > INT_MAX) | (F.abs("_good_raw") > INT_MAX)).count()
    if bad:
        raise ArithmeticError(f"{bad} groups overflow INT (Arithmetic overflow in legacy)")


def transform(feeds: dict) -> DataFrame:
    """Raw feeds (keyed by schema.table) -> rpt.daily_production rows."""
    sc = build_shift_calendar(feeds["dim.plant"], feeds["dim.shift_pattern"],
                              feeds["dim.calendar"])
    stg = stage_production_local(feeds["mes.production_count"], feeds["dim.line"],
                                 feeds["dim.plant"], sc)
    return build_report(stg, feeds["dim.sku"]).select(*OUTPUT_COLUMNS)


def run(ns: str, as_of_utc: str, spark: SparkSession = None):
    spark = spark or get_spark(f"mfg_lake.{REPORT}")
    feeds = {t: read_raw(spark, t) for t in FEEDS}
    pc = feeds["mes.production_count"]
    n_in = pc.count()
    n_null = n_in - pc.dropna(subset=list(NOT_NULL)).count()
    sc = build_shift_calendar(feeds["dim.plant"], feeds["dim.shift_pattern"],
                              feeds["dim.calendar"])
    stg = stage_production_local(pc, feeds["dim.line"], feeds["dim.plant"], sc).cache()
    rpt = build_report(stg, feeds["dim.sku"]).cache()
    _check_int_overflow(rpt)
    out = rpt.select(*OUTPUT_COLUMNS).orderBy("plant_id", "line_id", "production_day", "sku_id")
    dest = write_curated(out, REPORT, ns)
    print(f"[{REPORT}] as_of_utc={as_of_utc} (not applied, as legacy) "
          f"production_count={n_in} dropped_not_null={n_null} "
          f"shift_calendar={sc.count()} staged={stg.count()} rows={rpt.count()}")
    print(f"[{REPORT}] wrote {abfss_uri(REPORT)} -> {dest}")
    stg.unpersist()
    rpt.unpersist()
    return dest


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ns", required=True, help="lake namespace (local: out/<ns>)")
    ap.add_argument("--as-of-utc", required=True,
                    help="report cutoff 'YYYY-MM-DD HH:MM:SS' (accepted, not applied)")
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
