"""PL_Daily_Production -> curated daily_production (PySpark conversion).

Replaces mes.usp_stg_production_counts -> rpt.usp_rpt_daily_production ->
COPY_rpt_daily_production_to_curated.

Usage:
    python -m mfg_lake.jobs.daily_production --ns <ns> --as-of-utc <cutoff>
"""
import argparse
import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.shift_calendar import build_shift_calendar, plants_with_iana
from mfg_lake.common.spark import get_spark
from mfg_lake.common.timeconv import datediff_minute, parse_as_of_utc, to_local, ts_seconds

REPORT = "daily_production"
SOURCES = ["mes.production_count", "dim.line", "dim.plant", "dim.shift_pattern",
           "dim.calendar", "dim.sku"]
GRAIN = ["plant_id", "line_id", "production_day", "sku_id"]
OUTPUT_COLUMNS = GRAIN + ["total_units", "good_units", "cases", "yield_pct"]
STG_PRODUCTION_LOCAL_COLUMNS = [
    "line_id", "plant_id", "sku_id", "production_day", "shift_code", "bucket_minutes",
    "total_units", "good_units",
]
INT_MIN, INT_MAX = -2**31, 2**31 - 1

log = logging.getLogger("mfg_lake.jobs.daily_production")


def ufn_production_day(local_col) -> Column:
    """dim.ufn_production_day: label the local timestamp minus six hours."""
    return F.to_date(local_col - F.expr("INTERVAL 6 HOURS"))


def _log_dropped(dropped: DataFrame, join_name: str) -> int:
    n = dropped.count()
    if n:
        samples = [tuple(row[c] for c in ("line_id", "bucket_start_utc", "sku_id"))
                   for row in dropped.select("line_id", "bucket_start_utc", "sku_id")
                   .limit(5).collect()]
        log.warning("dropped %d bucket(s) at %s inner join; sample keys=%s",
                    n, join_name, samples)
    else:
        log.info("dropped 0 bucket(s) at %s inner join", join_name)
    return n


def stg_production_local(production_count: DataFrame, line: DataFrame, plants: DataFrame,
                         shift_calendar: DataFrame) -> DataFrame:
    """Build the reusable stg.production_local stage.

    ``plants`` must be ``plants_with_iana(dim.plant)`` and ``shift_calendar``
    must be ``build_shift_calendar(...)``. Buckets are joined by their UTC
    start instant and attributed to the plant-local production day.
    """
    c = (production_count.select(
        F.trim("line_id").alias("line_id"),
        F.trim("sku_id").alias("sku_id"),
        ts_seconds("bucket_start_utc").alias("bucket_start_utc"),
        ts_seconds("bucket_end_utc").alias("bucket_end_utc"),
        F.col("total_units").cast("int").alias("total_units"),
        F.col("good_units").cast("int").alias("good_units"))
         .withColumn("_bucket_id", F.monotonically_increasing_id()).cache())
    ln = line.select(F.trim("line_id").alias("line_id"),
                     F.trim("plant_id").alias("plant_id"))
    _log_dropped(c.join(ln.select("line_id").distinct(), "line_id", "left_anti"),
                 "dim.line")
    with_line = (c.alias("c").join(ln.alias("l"),
                                   F.col("c.line_id") == F.col("l.line_id"))
                 .select("c.*", F.col("l.plant_id").alias("plant_id")))

    _log_dropped(with_line.join(plants.select("plant_id").distinct(), "plant_id", "left_anti"),
                 "dim.plant")
    with_plant = with_line.join(plants, "plant_id")

    sc = (shift_calendar.withColumn("_shift_id", F.monotonically_increasing_id())
          .cache())
    b, s = with_plant.alias("b"), sc.alias("sc")
    shift_condition = ((F.col("b.plant_id") == F.col("sc.plant_id"))
                       & (F.col("b.bucket_start_utc") >= F.col("sc.start_utc"))
                       & (F.col("b.bucket_start_utc") < F.col("sc.end_utc")))
    _log_dropped(b.join(s, shift_condition, "left_anti"), "dim.shift_calendar")
    joined = (b.join(s, shift_condition)
              .select("b.*", F.col("sc.shift_code").alias("shift_code"),
                      F.col("sc._shift_id").alias("_shift_id")))
    multi = (joined.groupBy("_bucket_id").agg(F.countDistinct("_shift_id").alias("n_shifts"))
             .where(F.col("n_shifts") > 1).count())
    if multi:
        log.warning("matched %d bucket(s) to multiple shift_calendar rows; keeping duplicates",
                    multi)

    return (joined
            .withColumn("production_day",
                        ufn_production_day(to_local(F.col("bucket_start_utc"),
                                                    F.col("iana"))))
            .withColumn("bucket_minutes",
                        datediff_minute(F.col("bucket_start_utc"),
                                        F.col("bucket_end_utc")))
            .select(
                F.col("line_id").cast("string").alias("line_id"),
                F.col("plant_id").cast("string").alias("plant_id"),
                F.col("sku_id").cast("string").alias("sku_id"),
                F.col("production_day").cast("date").alias("production_day"),
                F.col("shift_code").cast("string").alias("shift_code"),
                F.col("bucket_minutes").cast("int").alias("bucket_minutes"),
                F.col("total_units").cast("int").alias("total_units"),
                F.col("good_units").cast("int").alias("good_units")))


def rpt_daily_production(stg: DataFrame, sku: DataFrame) -> DataFrame:
    """Build rpt.daily_production, preserving SQL Server integer/decimal math."""
    k = sku.select(F.trim("sku_id").alias("sku_id"),
                   F.col("pack_size").cast("int").alias("pack_size"))
    dropped = stg.join(k.select("sku_id").distinct(), "sku_id", "left_anti")
    n_dropped = dropped.count()
    if n_dropped:
        sample_skus = sorted(row.sku_id for row in
                             dropped.select("sku_id").distinct().limit(5).collect())
        log.warning("dropped %d stage row(s) at dim.sku inner join; sample sku_ids=%s",
                    n_dropped, sample_skus)
    else:
        log.info("dropped 0 stage row(s) at dim.sku inner join")
    joined = stg.join(k, "sku_id")

    zero_skus = sorted(row.sku_id for row in
                       joined.where(F.col("pack_size") == 0)
                       .select("sku_id").distinct().collect())
    if zero_skus:
        raise ZeroDivisionError(
            f"Divide by zero error encountered: dim.sku.pack_size = 0 for sku_id(s) {zero_skus}")

    aggregated = (joined.groupBy(*GRAIN, "pack_size")
                  .agg(F.sum("total_units").cast("bigint").alias("total_units"),
                       F.sum("good_units").cast("bigint").alias("good_units"))
                  .cache())
    overflow = aggregated.where(
        (F.col("total_units") < INT_MIN) | (F.col("total_units") > INT_MAX)
        | (F.col("good_units") < INT_MIN) | (F.col("good_units") > INT_MAX)
    ).limit(1).count()
    if overflow:
        raise OverflowError("Arithmetic overflow converting expression to data type int")

    yield_ratio = (
        F.col("good_units").cast("decimal(18,4)")
        / F.when(F.col("total_units") != 0, F.col("total_units")).cast("decimal(10,0)")
        * F.lit(100).cast("decimal(3,0)"))
    return (aggregated
            .withColumn("cases", F.expr("good_units div pack_size").cast("int"))
            .withColumn("yield_pct", F.round(yield_ratio, 2).cast("decimal(9,2)"))
            .select(
                F.col("plant_id").cast("string").alias("plant_id"),
                F.col("line_id").cast("string").alias("line_id"),
                F.col("production_day").cast("date").alias("production_day"),
                F.col("sku_id").cast("string").alias("sku_id"),
                F.col("total_units").cast("int").alias("total_units"),
                F.col("good_units").cast("int").alias("good_units"),
                F.col("cases").cast("int").alias("cases"),
                F.col("yield_pct").cast("decimal(9,2)").alias("yield_pct"))
            .select(*OUTPUT_COLUMNS))


def build_report(frames: dict, as_of_utc: str) -> DataFrame:
    """frames: {schema.table: DataFrame} for SOURCES -> report DataFrame."""
    as_of_utc = parse_as_of_utc(as_of_utc)
    log.info("legacy production procs do not read AsOfUtc; no cutoff filter is applied")
    plants = plants_with_iana(frames["dim.plant"]).cache()
    shift_calendar = build_shift_calendar(
        plants, frames["dim.shift_pattern"], frames["dim.calendar"]).cache()
    stg = stg_production_local(frames["mes.production_count"], frames["dim.line"],
                               plants, shift_calendar).cache()
    n_source = frames["mes.production_count"].count()
    n_staged = stg.count()
    log.info("stage 1 stg.production_local: %d staged bucket(s) of %d source bucket(s)",
             n_staged, n_source)
    return rpt_daily_production(stg, frames["dim.sku"])


def run(spark: SparkSession, ns: str, as_of_utc: str):
    frames = {table: read_raw(spark, table) for table in SOURCES}
    out = build_report(frames, as_of_utc).cache()
    n = out.count()
    dest = write_curated(out, REPORT, ns, mode="overwrite")
    log.info("stage 2 %s: wrote %d row(s) -> %s", REPORT, n, dest)
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
