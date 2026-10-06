"""PL_OEE -> curated oee_shift (PySpark conversion).

Replaces SP_CalcPlannedTime -> SP_RptOeeShift ->
COPY_rpt_oee_shift_to_curated.

Usage:
    python -m mfg_lake.jobs.oee_shift --ns <ns> --as-of-utc <cutoff>
"""
import argparse
import logging
from datetime import date
from decimal import Decimal

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.shift_calendar import build_shift_calendar, plants_with_iana
from mfg_lake.common.spark import get_spark
from mfg_lake.common.timeconv import datediff_minute, parse_as_of_utc
from mfg_lake.jobs.daily_production import stg_production_local
from mfg_lake.jobs.line_downtime_daily import stage_downtime_local, split_by_shift

REPORT = "oee_shift"
SOURCES = ["mes.downtime_event", "mes.production_count", "dim.line", "dim.plant",
           "dim.shift_pattern", "dim.calendar", "dim.sku"]
GRAIN = ["plant_id", "line_id", "production_day", "shift_code"]
OUTPUT_COLUMNS = GRAIN + ["planned_min", "unplanned_dt_min", "availability",
                          "performance", "quality", "oee"]
PLANNED_TIME_COLUMNS = GRAIN + ["shift_minutes", "planned_dt_min", "unplanned_dt_min"]
LEGACY_MIN_PRODUCTION_DAY = date(2025, 10, 20)
INT_MIN, INT_MAX = -2**31, 2**31 - 1
DECIMAL_9_4_MAX = Decimal("99999.9999")

log = logging.getLogger("mfg_lake.jobs.oee_shift")


def _round_ratio_half_away(num_col: str, den_col: str, scale: int = 4) -> Column:
    """Round an exact integral ratio half away from zero."""
    if not 0 <= scale <= 36:
        raise ValueError("scale must be between 0 and 36")
    num = f"CAST(`{num_col}` AS DECIMAL(38,0))"
    den = f"CAST(`{den_col}` AS DECIMAL(38,0))"
    denominator = f"(2 * ABS({den}))"
    rounded_magnitude = (
        f"((2 * ABS({num}) * {10**scale} + ABS({den})) "
        f"div NULLIF({denominator}, 0))")
    sign = (
        f"(CASE WHEN {num} < 0 THEN -1 ELSE 1 END) * "
        f"(CASE WHEN {den} < 0 THEN -1 ELSE 1 END)")
    factor = format(Decimal(1).scaleb(-scale), "f")
    factor_precision = max(scale, 1)
    quotient_precision = 36 if scale == 0 else 37 - scale
    return F.expr(f"""
        CASE
            WHEN `{num_col}` IS NULL OR `{den_col}` IS NULL OR {den} = 0
                THEN CAST(NULL AS DECIMAL(38,{scale}))
            ELSE CAST(
                CAST(({sign}) * {rounded_magnitude}
                     AS DECIMAL({quotient_precision},0))
                * CAST({factor} AS DECIMAL({factor_precision},{scale}))
                AS DECIMAL(38,{scale}))
        END
    """)


def _sample_keys(df: DataFrame, columns: list[str]) -> list[tuple]:
    return [tuple(row[column] for column in columns)
            for row in df.select(*columns).limit(5).collect()]


def stg_planned_time(shift_calendar: DataFrame, line: DataFrame,
                     seg: DataFrame) -> DataFrame:
    """Build stg.planned_time with the legacy calendar×line left join."""
    ln = line.select(F.trim("line_id").alias("line_id"),
                     F.trim("plant_id").alias("plant_id"))

    dropped_calendar = shift_calendar.join(
        ln.select("plant_id").distinct(), "plant_id", "left_anti")
    n_calendar_dropped = dropped_calendar.count()
    if n_calendar_dropped:
        log.warning(
            "dropped %d shift_calendar row(s) with no dim.line plant match; sample keys=%s",
            n_calendar_dropped,
            _sample_keys(dropped_calendar, ["plant_id", "production_day", "shift_code"]))
    else:
        log.info("dropped 0 shift_calendar row(s) with no dim.line plant match")

    sc = shift_calendar.alias("sc")
    ln_alias = ln.alias("ln")
    calendar_line = (sc.join(ln_alias, F.col("sc.plant_id") == F.col("ln.plant_id"))
                     .select(F.col("sc.plant_id").alias("plant_id"),
                             F.col("ln.line_id").alias("line_id"),
                             F.col("sc.production_day").alias("production_day"),
                             F.col("sc.shift_code").alias("shift_code"),
                             F.col("sc.start_utc").alias("start_utc"),
                             F.col("sc.end_utc").alias("end_utc")))

    dropped_segments = seg.join(
        calendar_line.select(*GRAIN).distinct(), GRAIN, "left_anti")
    n_segments_dropped = dropped_segments.count()
    if n_segments_dropped:
        log.warning(
            "dropped %d downtime segment(s) with no calendar×line match; sample keys=%s",
            n_segments_dropped,
            _sample_keys(dropped_segments,
                         ["plant_id", "line_id", "production_day", "shift_code", "event_id"]))
    else:
        log.info("dropped 0 downtime segment(s) with no calendar×line match")

    minutes = datediff_minute("seg_start_local", "seg_end_local")
    joined = calendar_line.join(seg, GRAIN, "left")
    aggregated = (joined
                  .withColumn("_shift_minutes", datediff_minute("start_utc", "end_utc"))
                  .withColumn("_segment_minutes", minutes.cast("bigint"))
                  .groupBy(*GRAIN, "start_utc", "end_utc")
                  .agg(F.sum(F.when(F.col("planned_flag"),
                                    F.col("_segment_minutes"))).cast("bigint")
                       .alias("planned_dt_min"),
                       F.sum(F.when(~F.col("planned_flag"),
                                    F.col("_segment_minutes"))).cast("bigint")
                       .alias("unplanned_dt_min"),
                       F.first("_shift_minutes").cast("bigint").alias("shift_minutes"))
                  .withColumn("planned_dt_min",
                              F.coalesce("planned_dt_min", F.lit(0).cast("bigint")))
                  .withColumn("unplanned_dt_min",
                              F.coalesce("unplanned_dt_min", F.lit(0).cast("bigint"))))

    overflow = aggregated.where(
        (F.col("shift_minutes") < INT_MIN) | (F.col("shift_minutes") > INT_MAX)
        | (F.col("planned_dt_min") < INT_MIN) | (F.col("planned_dt_min") > INT_MAX)
        | (F.col("unplanned_dt_min") < INT_MIN) | (F.col("unplanned_dt_min") > INT_MAX)
    ).limit(1).count()
    if overflow:
        raise OverflowError(
            "Arithmetic overflow error converting expression to data type int.")

    return (aggregated
            .select(F.col("plant_id").cast("string").alias("plant_id"),
                    F.col("line_id").cast("string").alias("line_id"),
                    F.col("production_day").cast("date").alias("production_day"),
                    F.col("shift_code").cast("string").alias("shift_code"),
                    F.col("shift_minutes").cast("int").alias("shift_minutes"),
                    F.col("planned_dt_min").cast("int").alias("planned_dt_min"),
                    F.col("unplanned_dt_min").cast("int").alias("unplanned_dt_min"))
            .select(*PLANNED_TIME_COLUMNS))


def rpt_oee_shift(planned_time: DataFrame, production_local: DataFrame,
                  sku: DataFrame) -> DataFrame:
    """Build rpt.oee_shift using ROUND(exact ratio, 4), half away from zero,
    matching the production extract.
    """
    k = sku.select(F.trim("sku_id").alias("sku_id"),
                   F.col("ideal_units_per_min").cast("decimal(9,2)")
                   .alias("ideal_units_per_min"))
    dropped_sku = production_local.join(k.select("sku_id").distinct(),
                                         "sku_id", "left_anti")
    n_sku_dropped = dropped_sku.count()
    if n_sku_dropped:
        sample_skus = sorted(row.sku_id for row in
                             dropped_sku.select("sku_id").distinct().limit(5).collect())
        log.warning(
            "dropped %d production_local row(s) at dim.sku inner join; sample sku_ids=%s",
            n_sku_dropped, sample_skus)
    else:
        log.info("dropped 0 production_local row(s) at dim.sku inner join")

    keyed_production = production_local.join(k, "sku_id")
    prod = (keyed_production.groupBy(*GRAIN)
            .agg(F.sum("total_units").cast("bigint").alias("total_units"),
                 F.sum("good_units").cast("bigint").alias("good_units"),
                 F.sum(F.col("ideal_units_per_min")
                       * F.col("bucket_minutes").cast("decimal(10,0)"))
                 .cast("decimal(38,2)").alias("ideal_units"))
            .cache())
    overflow = prod.where(
        (F.col("total_units") < INT_MIN) | (F.col("total_units") > INT_MAX)
        | (F.col("good_units") < INT_MIN) | (F.col("good_units") > INT_MAX)
    ).limit(1).count()
    if overflow:
        raise OverflowError(
            "Arithmetic overflow error converting expression to data type int.")
    prod = (prod.withColumn("total_units", F.col("total_units").cast("int"))
            .withColumn("good_units", F.col("good_units").cast("int")))

    n_filtered = planned_time.where(
        F.col("production_day") < F.lit(LEGACY_MIN_PRODUCTION_DAY)).count()
    log.info("filtered %d planned_time row(s) before %s",
             n_filtered, LEGACY_MIN_PRODUCTION_DAY)
    pt = planned_time.where(
        F.col("production_day") >= F.lit(LEGACY_MIN_PRODUCTION_DAY))

    dropped_pt = pt.join(prod.select(*GRAIN).distinct(), GRAIN, "left_anti")
    n_pt_dropped = dropped_pt.count()
    if n_pt_dropped:
        log.warning("dropped %d planned_time row(s) with no production row; sample keys=%s",
                    n_pt_dropped, _sample_keys(dropped_pt, GRAIN))
    else:
        log.info("dropped 0 planned_time row(s) with no production row")

    dropped_prod = prod.join(pt.select(*GRAIN).distinct(), GRAIN, "left_anti")
    n_prod_dropped = dropped_prod.count()
    if n_prod_dropped:
        log.warning("dropped %d production row(s) with no planned_time row; sample keys=%s",
                    n_prod_dropped, _sample_keys(dropped_prod, GRAIN))
    else:
        log.info("dropped 0 production row(s) with no planned_time row")

    joined = (pt.alias("pt").join(prod.alias("pr"), GRAIN)
              .select(*[F.col(f"pt.{column}").alias(column) for column in GRAIN],
                      F.col("pt.shift_minutes").alias("shift_minutes"),
                      F.col("pt.unplanned_dt_min").alias("unplanned_dt_min"),
                      F.col("pr.total_units").alias("total_units"),
                      F.col("pr.good_units").alias("good_units"),
                      F.col("pr.ideal_units").alias("ideal_units")))

    ratio_inputs = (joined
                    .withColumn("_sm", F.col("shift_minutes").cast("decimal(38,0)"))
                    .withColumn("_udt", F.col("unplanned_dt_min").cast("decimal(38,0)"))
                    .withColumn("_total", F.col("total_units").cast("decimal(38,0)"))
                    .withColumn("_good", F.col("good_units").cast("decimal(38,0)"))
                    .withColumn("_ideal100",
                                (F.col("ideal_units") * F.lit(100))
                                .cast("decimal(38,0)"))
                    .withColumn("_up", (F.col("_sm") - F.col("_udt"))
                                .cast("decimal(38,0)"))
                    .withColumn("_performance_num",
                                (F.col("_total") * F.lit(100))
                                .cast("decimal(38,0)"))
                    .withColumn("_oee_num",
                                (F.col("_up") * F.col("_good") * F.lit(100))
                                .cast("decimal(38,0)"))
                    .withColumn("_oee_den",
                                (F.col("_sm") * F.col("_ideal100"))
                                .cast("decimal(38,0)")))
    metrics = (ratio_inputs
               .withColumn("availability",
                           _round_ratio_half_away("_up", "_sm"))
               .withColumn("performance",
                           _round_ratio_half_away("_performance_num", "_ideal100"))
               .withColumn("quality",
                           _round_ratio_half_away("_good", "_total"))
               .withColumn("oee", F.when(
                   F.col("_total").isNotNull() & (F.col("_total") != 0),
                   _round_ratio_half_away("_oee_num", "_oee_den")))
               )
    out_of_range = F.lit(False)
    for name in ("availability", "performance", "quality", "oee"):
        out_of_range = out_of_range | (
            F.abs(F.col(name)) > F.lit(DECIMAL_9_4_MAX))
    if metrics.where(out_of_range).limit(1).count():
        raise OverflowError(
            "Arithmetic overflow error converting numeric to data type numeric.")

    return (metrics.select(
        F.col("plant_id").cast("string").alias("plant_id"),
        F.col("line_id").cast("string").alias("line_id"),
        F.col("production_day").cast("date").alias("production_day"),
        F.col("shift_code").cast("string").alias("shift_code"),
        F.col("shift_minutes").cast("int").alias("planned_min"),
        F.col("unplanned_dt_min").cast("int").alias("unplanned_dt_min"),
        F.col("availability").cast("decimal(9,4)").alias("availability"),
        F.col("performance").cast("decimal(9,4)").alias("performance"),
        F.col("quality").cast("decimal(9,4)").alias("quality"),
        F.col("oee").cast("decimal(9,4)").alias("oee"))
            .select(*OUTPUT_COLUMNS))


def build_report(frames: dict, as_of_utc: str) -> DataFrame:
    """Build curated oee_shift from source DataFrames keyed by schema.table."""
    as_of = parse_as_of_utc(as_of_utc)
    log.info("OEE stored procedures do not read AsOfUtc; the downtime segment stage does")
    plants = plants_with_iana(frames["dim.plant"]).cache()
    shift_calendar = build_shift_calendar(
        plants, frames["dim.shift_pattern"], frames["dim.calendar"]).cache()

    local = stage_downtime_local(frames["mes.downtime_event"], frames["dim.line"],
                                 plants, as_of).cache()
    source_events = frames["mes.downtime_event"].count()
    kept_events = local.count()
    capped_events = local.where("capped_at_as_of").count()
    log.info("stage stg.downtime_local: kept %d of %d source event(s); %d capped at AsOfUtc",
             kept_events, source_events, capped_events)

    seg = split_by_shift(local.drop("capped_at_as_of"), shift_calendar).cache()
    production_local = stg_production_local(
        frames["mes.production_count"], frames["dim.line"], plants, shift_calendar).cache()
    planned_time = stg_planned_time(shift_calendar, frames["dim.line"], seg).cache()
    log.info("stage stg.downtime_shift_seg: %d row(s)", seg.count())
    log.info("stage stg.production_local: %d row(s)", production_local.count())
    log.info("stage stg.planned_time: %d row(s)", planned_time.count())
    return rpt_oee_shift(planned_time, production_local, frames["dim.sku"])


def run(spark: SparkSession, ns: str, as_of_utc: str):
    frames = {table: read_raw(spark, table) for table in SOURCES}
    out = build_report(frames, as_of_utc).cache()
    n = out.count()
    dest = write_curated(out, REPORT, ns, mode="overwrite")
    log.info("wrote %d row(s) to %s -> %s", n, REPORT, dest)
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
