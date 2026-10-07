"""PL_OEE -> curated OEE metrics per line and shift.

    python -m mfg_lake.jobs.oee_shift --ns <ns> --as-of-utc <cutoff>

The cutoff is applied only by the imported downtime stage, as in PL_OEE's
upstream PL_Line_Downtime flow.
"""
import argparse
import sys
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.paths import abfss_uri
from mfg_lake.common.spark import get_spark
from mfg_lake.jobs.daily_production import (
    build_shift_calendar,
    stage_production_local,
)
from mfg_lake.jobs.line_downtime_daily import (
    datediff_minute,
    split_downtime_by_shift,
    stage_downtime_local,
)

REPORT = "oee_shift"
FEEDS = ("mes.downtime_event", "dim.line", "dim.plant", "dim.shift_pattern",
         "dim.calendar", "mes.production_count", "dim.sku")
PRODUCTION_NOT_NULL = ("line_id", "bucket_start_utc", "bucket_end_utc", "sku_id",
                       "total_units", "good_units")
DOWNTIME_NOT_NULL = ("event_id", "line_id", "start_utc", "reason_code", "planned_flag")
KEYS = ("plant_id", "line_id", "production_day", "shift_code")
OUTPUT_COLUMNS = (*KEYS, "planned_min", "unplanned_dt_min", "availability",
                  "performance", "quality", "oee")
INT_MIN, INT_MAX = -(2**31), 2**31 - 1
IDEAL_SCALE = 2
METRIC_SCALE = 4
METRIC_FACTOR = 10**METRIC_SCALE
# Legacy rpt.usp_rpt_oee_shift hard-codes production_day >= '2025-10-20'.
PRODUCTION_DAY_FLOOR = date(2025, 10, 20)


def _int_checked(value: Column, label: str) -> Column:
    bad = (value < INT_MIN) | (value > INT_MAX)
    return F.when(
        bad, F.raise_error(F.lit(f"{label} overflows SQL INT"))
    ).otherwise(value.cast("int"))


def _guard_null_overflow(df: DataFrame, name: str, inputs: tuple[str, ...],
                         message: str) -> DataFrame:
    valid_inputs = F.lit(True)
    for input_name in inputs:
        valid_inputs = valid_inputs & F.col(input_name).isNotNull()
    return df.withColumn(
        name,
        F.when(valid_inputs & F.col(name).isNull(), F.raise_error(F.lit(message)))
        .otherwise(F.col(name)),
    )


def calc_planned_time(shift_calendar: DataFrame, line: DataFrame,
                      downtime_shift_seg: DataFrame) -> DataFrame:
    """mes.usp_calc_planned_time -> planned shift and downtime minutes."""
    sc = shift_calendar.select(
        F.trim(F.col("plant_id").cast("string")).alias("plant_id"),
        F.col("production_day").cast("date").alias("production_day"),
        F.trim(F.col("shift_code").cast("string")).alias("shift_code"),
        F.col("start_utc").cast("timestamp").alias("start_utc"),
        F.col("end_utc").cast("timestamp").alias("end_utc"),
    ).alias("sc")
    ln = line.select(
        F.trim(F.col("line_id").cast("string")).alias("line_id"),
        F.trim(F.col("plant_id").cast("string")).alias("plant_id"),
    ).alias("ln")
    seg = downtime_shift_seg.select(
        F.trim(F.col("plant_id").cast("string")).alias("plant_id"),
        F.trim(F.col("line_id").cast("string")).alias("line_id"),
        F.col("production_day").cast("date").alias("production_day"),
        F.trim(F.col("shift_code").cast("string")).alias("shift_code"),
        F.col("planned_flag").cast("boolean").alias("planned_flag"),
        F.col("seg_start_local").cast("timestamp").alias("seg_start_local"),
        F.col("seg_end_local").cast("timestamp").alias("seg_end_local"),
    ).alias("seg")
    calendar_lines = (sc.join(ln, F.col("sc.plant_id") == F.col("ln.plant_id"), "inner")
                      .select(
                          F.col("sc.plant_id").alias("plant_id"),
                          F.col("ln.line_id").alias("line_id"),
                          F.col("sc.production_day").alias("production_day"),
                          F.col("sc.shift_code").alias("shift_code"),
                          F.col("sc.start_utc").alias("start_utc"),
                          F.col("sc.end_utc").alias("end_utc"),
                      ).alias("cl"))
    joined = calendar_lines.join(
        seg,
        (F.col("cl.plant_id") == F.col("seg.plant_id"))
        & (F.col("cl.line_id") == F.col("seg.line_id"))
        & (F.col("cl.production_day") == F.col("seg.production_day"))
        & (F.col("cl.shift_code") == F.col("seg.shift_code")),
        "left",
    )
    grouped = joined.groupBy(
        F.col("cl.plant_id").alias("plant_id"),
        F.col("cl.line_id").alias("line_id"),
        F.col("cl.production_day").alias("production_day"),
        F.col("cl.shift_code").alias("shift_code"),
        F.col("cl.start_utc").alias("start_utc"),
        F.col("cl.end_utc").alias("end_utc"),
    ).agg(
        F.sum(F.when(F.col("seg.planned_flag"),
                     datediff_minute("seg.seg_start_local", "seg.seg_end_local"))
              .otherwise(F.lit(0))).alias("_planned_dt_raw"),
        F.sum(F.when(F.col("seg.planned_flag") == F.lit(False),
                     datediff_minute("seg.seg_start_local", "seg.seg_end_local"))
              .otherwise(F.lit(0))).alias("_unplanned_dt_raw"),
    )
    grouped = (grouped
               .withColumn("_planned_dt_raw", F.coalesce("_planned_dt_raw", F.lit(0)))
               .withColumn("_unplanned_dt_raw", F.coalesce("_unplanned_dt_raw", F.lit(0)))
               .withColumn("_shift_raw", datediff_minute("start_utc", "end_utc")))
    return grouped.select(
        F.col("plant_id"),
        F.col("line_id"),
        F.col("production_day"),
        F.col("shift_code"),
        F.col("_shift_raw").cast("int").alias("shift_minutes"),
        _int_checked(F.col("_planned_dt_raw"), "planned_dt_min").alias("planned_dt_min"),
        _int_checked(F.col("_unplanned_dt_raw"), "unplanned_dt_min").alias("unplanned_dt_min"),
    )


def _round_half_away(numerator: str, denominator: str, scale: int = METRIC_SCALE) -> Column:
    """Round an exact signed integer ratio to a decimal at the requested scale."""
    num = F.col(numerator).cast("decimal(38,0)")
    den = F.col(denominator).cast("decimal(38,0)")
    valid = num.isNotNull() & den.isNotNull() & (den != 0)
    factor = METRIC_FACTOR if scale == METRIC_SCALE else 10**scale
    magnitude_expr = F.expr(
        f"((2 * abs(`{numerator}`) * {factor} + abs(`{denominator}`)) "
        f"div (2 * abs(`{denominator}`)))"
    )
    magnitude = F.when(valid, magnitude_expr)
    signed = F.when(
        valid & ((num < 0) != (den < 0)), -magnitude
    ).otherwise(magnitude)
    too_large = valid & (F.abs(signed) > 999_999_999)
    signed = F.when(
        too_large, F.raise_error(F.lit("metric overflows DECIMAL(9,4)"))
    ).otherwise(signed)
    decimal_value = (
        signed.cast("decimal(18,0)")
        / F.lit(factor).cast("decimal(5,0)")
    ).cast("decimal(9,4)")
    return F.when(
        valid & decimal_value.isNull(),
        F.raise_error(F.lit("metric arithmetic overflow")),
    ).otherwise(decimal_value)


def _product(df: DataFrame, name: str, factors: tuple[str, ...]) -> DataFrame:
    value = F.col(factors[0]).cast("decimal(38,0)")
    for factor in factors[1:]:
        value = value * F.col(factor).cast("decimal(38,0)")
    df = df.withColumn(name, value.cast("decimal(38,0)"))
    return _guard_null_overflow(df, name, factors, f"{name} arithmetic overflow")


def _production_groups(production_local: DataFrame, sku: DataFrame) -> DataFrame:
    prod = production_local.select(
        F.trim(F.col("plant_id").cast("string")).alias("plant_id"),
        F.trim(F.col("line_id").cast("string")).alias("line_id"),
        F.col("production_day").cast("date").alias("production_day"),
        F.trim(F.col("shift_code").cast("string")).alias("shift_code"),
        F.trim(F.col("sku_id").cast("string")).alias("sku_id"),
        F.col("bucket_minutes").cast("int").alias("bucket_minutes"),
        F.col("total_units").cast("int").alias("total_units"),
        F.col("good_units").cast("int").alias("good_units"),
    )
    rates = sku.select(
        F.trim(F.col("sku_id").cast("string")).alias("sku_id"),
        F.col("ideal_units_per_min").cast("decimal(9,2)").alias("ideal_units_per_min"),
    )
    matched = prod.join(rates, "sku_id", "inner")
    rate_scaled = (
        F.col("ideal_units_per_min") * F.lit(10**IDEAL_SCALE).cast("decimal(3,0)")
    ).cast("decimal(12,0)")
    bucket_decimal = F.col("bucket_minutes").cast("decimal(10,0)")
    ideal_piece = (rate_scaled * bucket_decimal).cast("decimal(23,0)")
    groups = matched.groupBy(*KEYS).agg(
        F.sum(F.col("total_units").cast("decimal(38,0)")).alias("_total_raw"),
        F.sum(F.col("good_units").cast("decimal(38,0)")).alias("_good_raw"),
        F.sum(ideal_piece).alias("_ideal_scaled_raw"),
        F.count(F.lit(1)).alias("_bucket_count"),
    )
    groups = groups.withColumn(
        "_total_raw",
        F.when(F.col("_total_raw").isNull(),
               F.raise_error(F.lit("total_units aggregate overflow")))
        .otherwise(F.col("_total_raw")),
    ).withColumn(
        "_good_raw",
        F.when(F.col("_good_raw").isNull(),
               F.raise_error(F.lit("good_units aggregate overflow")))
        .otherwise(F.col("_good_raw")),
    ).withColumn(
        "_ideal_scaled_raw",
        F.when((F.col("_bucket_count") > 0) & F.col("_ideal_scaled_raw").isNull(),
               F.raise_error(F.lit("ideal_units aggregate overflow")))
        .otherwise(F.col("_ideal_scaled_raw")),
    )
    return groups.select(
        *KEYS,
        _int_checked(F.col("_total_raw"), "total_units").alias("total_units"),
        _int_checked(F.col("_good_raw"), "good_units").alias("good_units"),
        F.col("_ideal_scaled_raw").cast("decimal(38,0)").alias("ideal_scaled"),
    )


def rpt_oee_shift(planned_time: DataFrame, production_local: DataFrame,
                  sku: DataFrame) -> DataFrame:
    """rpt.usp_rpt_oee_shift, with exact integer-ratio rounding."""
    planned = planned_time.select(
        F.trim(F.col("plant_id").cast("string")).alias("plant_id"),
        F.trim(F.col("line_id").cast("string")).alias("line_id"),
        F.col("production_day").cast("date").alias("production_day"),
        F.trim(F.col("shift_code").cast("string")).alias("shift_code"),
        F.col("shift_minutes").cast("long").alias("_shift_raw"),
        F.col("unplanned_dt_min").cast("long").alias("_unplanned_raw"),
    )
    prod = _production_groups(production_local, sku)
    joined = (planned.join(prod, list(KEYS), "inner")
              .filter(F.col("production_day") >= F.lit(PRODUCTION_DAY_FLOOR)))
    joined = (joined
              .withColumn("_shift", F.col("_shift_raw").cast("decimal(12,0)"))
              .withColumn("_unplanned", F.col("_unplanned_raw").cast("decimal(12,0)"))
              .withColumn("_runtime", (F.col("_shift") - F.col("_unplanned"))
                          .cast("decimal(13,0)"))
              .withColumn("_total", F.col("total_units").cast("decimal(12,0)"))
              .withColumn("_good", F.col("good_units").cast("decimal(12,0)"))
              .withColumn("_ideal", F.col("ideal_scaled").cast("decimal(38,0)")))
    joined = _product(joined, "_performance_num", ("_total",))
    joined = joined.withColumn(
        "_performance_num", (F.col("_performance_num") * F.lit(100))
        .cast("decimal(38,0)"))
    joined = _guard_null_overflow(
        joined, "_performance_num", ("_total",), "performance numerator overflow")
    joined = _product(joined, "_oee_num", ("_runtime", "_good"))
    joined = joined.withColumn(
        "_oee_num", (F.col("_oee_num") * F.lit(100)).cast("decimal(38,0)"))
    joined = _guard_null_overflow(
        joined, "_oee_num", ("_runtime", "_good"), "OEE numerator overflow")
    joined = _product(joined, "_oee_den", ("_shift", "_ideal"))
    joined = joined.withColumn(
        "_oee_num", F.when(F.col("_total") != 0, F.col("_oee_num")))
    joined = joined.withColumn(
        "_oee_den", F.when(F.col("_total") != 0, F.col("_oee_den")))
    joined = (joined
              .withColumn("_availability_num", F.col("_runtime"))
              .withColumn("_availability_den", F.col("_shift"))
              .withColumn("_performance_den", F.col("_ideal"))
              .withColumn("_quality_num", F.col("_good"))
              .withColumn("_quality_den", F.col("_total")))
    joined = (joined
              .withColumn("_availability", _round_half_away(
                  "_availability_num", "_availability_den"))
              .withColumn("_performance", _round_half_away(
                  "_performance_num", "_performance_den"))
              .withColumn("_quality", _round_half_away(
                  "_quality_num", "_quality_den"))
              # T cancels in the exact product ratio, but zero T still NULLs OEE
              # because the legacy expression includes NULLIF(total_units, 0).
              .withColumn("_oee", _round_half_away("_oee_num", "_oee_den")))
    return joined.select(
        F.col("plant_id").cast("string").alias("plant_id"),
        F.col("line_id").cast("string").alias("line_id"),
        F.col("production_day").cast("date").alias("production_day"),
        F.col("shift_code").cast("string").alias("shift_code"),
        _int_checked(F.col("_shift_raw"), "planned_min").alias("planned_min"),
        _int_checked(F.col("_unplanned_raw"), "unplanned_dt_min")
        .alias("unplanned_dt_min"),
        F.col("_availability").cast("decimal(9,4)").alias("availability"),
        F.col("_performance").cast("decimal(9,4)").alias("performance"),
        F.col("_quality").cast("decimal(9,4)").alias("quality"),
        F.col("_oee").cast("decimal(9,4)").alias("oee"),
    )


def transform(feeds: dict, as_of_utc) -> DataFrame:
    """Raw feeds keyed by schema.table -> rpt.oee_shift rows."""
    calendar = build_shift_calendar(
        feeds["dim.plant"], feeds["dim.shift_pattern"], feeds["dim.calendar"])
    downtime_local = stage_downtime_local(
        feeds["mes.downtime_event"], feeds["dim.line"], feeds["dim.plant"], as_of_utc)
    downtime_shift_seg = split_downtime_by_shift(
        downtime_local, feeds["dim.plant"], calendar)
    production_local = stage_production_local(
        feeds["mes.production_count"], feeds["dim.line"], feeds["dim.plant"], calendar)
    planned_time = calc_planned_time(calendar, feeds["dim.line"], downtime_shift_seg)
    return rpt_oee_shift(planned_time, production_local, feeds["dim.sku"]).select(
        *OUTPUT_COLUMNS)


def run(ns: str, as_of_utc: str, spark: SparkSession = None):
    spark = spark or get_spark(f"mfg_lake.{REPORT}")
    feeds = {table: read_raw(spark, table) for table in FEEDS}
    downtime_event = feeds["mes.downtime_event"]
    production_count = feeds["mes.production_count"]
    calendar = build_shift_calendar(
        feeds["dim.plant"], feeds["dim.shift_pattern"], feeds["dim.calendar"]).cache()
    downtime_local = stage_downtime_local(
        downtime_event, feeds["dim.line"], feeds["dim.plant"], as_of_utc).cache()
    downtime_shift_seg = split_downtime_by_shift(
        downtime_local, feeds["dim.plant"], calendar).cache()
    production_local = stage_production_local(
        production_count, feeds["dim.line"], feeds["dim.plant"], calendar).cache()
    planned_time = calc_planned_time(
        calendar, feeds["dim.line"], downtime_shift_seg).cache()
    result = rpt_oee_shift(planned_time, production_local, feeds["dim.sku"]).cache()
    out = result.select(*OUTPUT_COLUMNS).orderBy(*KEYS)
    dest = write_curated(out, REPORT, ns, mode="overwrite")

    event_clean = downtime_event.dropna(subset=list(DOWNTIME_NOT_NULL))
    production_clean = production_count.dropna(subset=list(PRODUCTION_NOT_NULL))
    line_plants = feeds["dim.line"].select(
        F.trim(F.col("plant_id")).alias("plant_id")).distinct()
    calendar_without_line = calendar.join(line_plants, "plant_id", "left_anti").count()
    sku_keys = feeds["dim.sku"].select(F.trim(F.col("sku_id")).alias("sku_id"))
    known_sku = production_local.join(sku_keys, "sku_id", "inner")
    unknown_sku = production_local.count() - known_sku.count()
    planned_keys = planned_time.select(*KEYS).distinct()
    prod_keys = _production_groups(production_local, feeds["dim.sku"]).select(*KEYS)
    pt_without_prod = planned_keys.join(prod_keys, list(KEYS), "left_anti").count()
    prod_without_pt = prod_keys.join(planned_keys, list(KEYS), "left_anti").count()
    date_dropped = (planned_keys.join(prod_keys, list(KEYS), "inner")
                    .filter(F.col("production_day") < F.lit(PRODUCTION_DAY_FLOOR)).count())

    print(f"[{REPORT}] as_of_utc={as_of_utc} "
          f"shift_calendar={calendar.count()} calendar_without_line={calendar_without_line} "
          f"downtime_events={downtime_event.count()} "
          f"dropped_downtime_not_null={downtime_event.count() - event_clean.count()} "
          f"downtime_local={downtime_local.count()} "
          f"downtime_shift_seg={downtime_shift_seg.count()} "
          f"production_count={production_count.count()} "
          f"dropped_production_not_null={production_count.count() - production_clean.count()} "
          f"production_local={production_local.count()} unknown_sku={unknown_sku} "
          f"planned_time={planned_time.count()} pt_without_prod={pt_without_prod} "
          f"prod_without_pt={prod_without_pt} date_filter={date_dropped} "
          f"rows={result.count()}")
    print(f"[{REPORT}] wrote {abfss_uri(REPORT)} -> {dest}")
    for frame in (calendar, downtime_local, downtime_shift_seg, production_local,
                  planned_time, result):
        frame.unpersist()
    return dest


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ns", required=True, help="lake namespace (local: out/<ns>)")
    parser.add_argument("--as-of-utc", required=True,
                        help="report cutoff 'YYYY-MM-DD HH:MM:SS' (AsOfUtc)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    spark = get_spark(f"mfg_lake.{REPORT}")
    try:
        run(args.ns, args.as_of_utc, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
