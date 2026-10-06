"""PL_Scrap_Yield -> curated scrap_yield_weekly (PySpark conversion).

Replaces SP_AllocateScrapToOrders -> SP_RptScrapYieldWeekly ->
COPY_rpt_scrap_yield_weekly_to_curated.

Usage:
    python -m mfg_lake.jobs.scrap_yield_weekly --ns <ns> --as-of-utc <cutoff>
"""
import argparse
import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.shift_calendar import build_shift_calendar, plants_with_iana
from mfg_lake.common.spark import get_spark
from mfg_lake.common.timeconv import parse_as_of_utc, ts_seconds
from mfg_lake.jobs.daily_production import INT_MAX, INT_MIN, stg_production_local

REPORT = "scrap_yield_weekly"
SOURCES = ["mes.scrap_event", "mes.production_order", "dim.line", "dim.plant",
           "dim.shift_pattern", "dim.calendar", "mes.production_count"]
GRAIN = ["plant_id", "line_id", "iso_year", "iso_week"]
OUTPUT_COLUMNS = GRAIN + ["good_units", "scrap_units", "scrap_pct"]

log = logging.getLogger("mfg_lake.jobs.scrap_yield_weekly")


def iso_week_and_year(d: Column) -> tuple[Column, Column]:
    """Return ISO year and week, matching the legacy DATEADD expression."""
    iso_week = F.weekofyear(d)
    iso_year = F.year(F.date_add(d, 26 - iso_week))
    return iso_year.cast("int"), iso_week.cast("int")


def _log_dropped_events(dropped: DataFrame) -> int:
    n = dropped.count()
    if n:
        sample_ids = [row.scrap_id for row in
                      dropped.select("scrap_id").limit(5).collect()]
        log.warning("dropped %d scrap event(s) at dim.line inner join; "
                    "sample scrap_ids=%s", n, sample_ids)
    else:
        log.info("dropped 0 scrap event(s) at dim.line inner join")
    return n


def _check_int_overflow(df: DataFrame, columns: list[str]) -> None:
    overflow = df.where(F.expr(" OR ".join(
        f"`{column}` < {INT_MIN} OR `{column}` > {INT_MAX}"
        for column in columns))).limit(1).count()
    if overflow:
        raise OverflowError("Arithmetic overflow converting expression to data type int")


def rpt_scrap_yield_weekly(scrap_event: DataFrame, line: DataFrame,
                           stg: DataFrame) -> DataFrame:
    """Build rpt.scrap_yield_weekly using the imported production-local stage."""
    events = (scrap_event.select(
        F.col("scrap_id").cast("int").alias("scrap_id"),
        F.trim("line_id").alias("line_id"),
        ts_seconds("scrap_ts_utc").alias("scrap_ts_utc"),
        F.col("qty_units").cast("int").alias("qty_units"))
        .cache())
    ln = line.select(F.trim("line_id").alias("line_id"),
                     F.trim("plant_id").alias("plant_id"))
    dropped = events.join(ln.select("line_id").distinct(), "line_id", "left_anti")
    n_dropped = _log_dropped_events(dropped)
    joined_events = (events.alias("s").join(
        ln.alias("l"), F.col("s.line_id") == F.col("l.line_id"))
        .select("s.scrap_id", F.col("l.plant_id").alias("plant_id"),
                "s.line_id", "s.scrap_ts_utc", "s.qty_units"))
    log.info("stage scrap_event: %d joined event(s) of %d source event(s)",
             events.count() - n_dropped, events.count())

    scrap_year, scrap_week = iso_week_and_year(F.to_date("scrap_ts_utc"))
    scrap = (joined_events
             .withColumn("iso_year", scrap_year)
             .withColumn("iso_week", scrap_week)
             .groupBy(*GRAIN)
             .agg(F.sum("qty_units").cast("bigint").alias("scrap_units"))
             .cache())
    _check_int_overflow(scrap, ["scrap_units"])

    good_year, good_week = iso_week_and_year(F.col("production_day"))
    good = (stg.select(F.trim("plant_id").alias("plant_id"),
                       F.trim("line_id").alias("line_id"),
                       F.col("good_units").cast("int").alias("good_units"),
                       F.col("production_day"))
            .withColumn("iso_year", good_year)
            .withColumn("iso_week", good_week)
            .groupBy(*GRAIN)
            .agg(F.sum("good_units").cast("bigint").alias("good_units"))
            .cache())
    _check_int_overflow(good, ["good_units"])

    dropped_groups = scrap.join(good.select(*GRAIN), GRAIN, "left_anti")
    n_dropped_groups = dropped_groups.count()
    if n_dropped_groups:
        sample_keys = sorted(tuple(row[column] for column in GRAIN)
                             for row in dropped_groups.select(*GRAIN)
                             .limit(5).collect())
        log.warning("dropped %d scrap group(s) with no good row at good LEFT JOIN; "
                    "sample keys=%s", n_dropped_groups, sample_keys)
    else:
        log.info("dropped 0 scrap group(s) with no good row at good LEFT JOIN")

    joined = (good.alias("g").join(
        scrap.alias("s"),
        [F.col(f"g.{column}") == F.col(f"s.{column}") for column in GRAIN],
        "left")
        .select(*[F.col(f"g.{column}").alias(column) for column in GRAIN],
                F.col("g.good_units").alias("good_units"),
                F.coalesce(F.col("s.scrap_units"), F.lit(0)).cast("bigint")
                .alias("scrap_units"))
        .withColumn("_denominator", F.col("good_units") + F.col("scrap_units")))
    _check_int_overflow(joined, ["_denominator"])

    scrap_pct = (
        F.col("scrap_units").cast("decimal(18,4)")
        / F.when(F.col("_denominator") != 0, F.col("_denominator"))
        .cast("decimal(10,0)")
        * F.lit(100).cast("decimal(3,0)"))
    return (joined
            .withColumn("scrap_pct", F.round(scrap_pct, 2).cast("decimal(9,2)"))
            .select(
                F.col("plant_id").cast("string").alias("plant_id"),
                F.col("line_id").cast("string").alias("line_id"),
                F.col("iso_year").cast("int").alias("iso_year"),
                F.col("iso_week").cast("int").alias("iso_week"),
                F.col("good_units").cast("int").alias("good_units"),
                F.col("scrap_units").cast("int").alias("scrap_units"),
                F.col("scrap_pct").cast("decimal(9,2)").alias("scrap_pct"))
            .select(*OUTPUT_COLUMNS))


def build_report(frames: dict, as_of_utc: str) -> DataFrame:
    """frames: {schema.table: DataFrame} for SOURCES -> report DataFrame."""
    parse_as_of_utc(as_of_utc)
    log.info("legacy scrap-yield procs do not read AsOfUtc; no cutoff filter is applied")
    plants = plants_with_iana(frames["dim.plant"]).cache()
    shift_calendar = build_shift_calendar(
        plants, frames["dim.shift_pattern"], frames["dim.calendar"]).cache()
    stg = stg_production_local(frames["mes.production_count"], frames["dim.line"],
                               plants, shift_calendar).cache()
    n_source = frames["mes.production_count"].count()
    n_staged = stg.count()
    log.info("stage stg.production_local: %d staged bucket(s) of %d source bucket(s)",
             n_staged, n_source)
    # stage: stg.scrap_alloc
    return rpt_scrap_yield_weekly(
        frames["mes.scrap_event"], frames["dim.line"], stg)


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
