"""PL_Scrap_Yield -> curated scrap_yield_weekly.

Replaces, in one Spark job:
  dim.usp_refresh_shift_calendar   (shift calendar, built in-job; no feed)
  mes.usp_stg_production_counts    (stg.production_local)
  mes.usp_allocate_scrap_to_orders (stg.scrap_alloc, computed + logged only:
                                  the rpt proc reads mes.scrap_event, not
                                  the staging table, and the Copy exports
                                  rpt only)
  rpt.usp_rpt_scrap_yield_weekly   (rpt.scrap_yield_weekly)

    python -m mfg_lake.jobs.scrap_yield_weekly --ns <ns> --as-of-utc <cutoff>

--as-of-utc is accepted for the job convention but, like the legacy
pipeline (which declares AsOfUtc and never passes it to the procs), does
not filter anything.

Grain: (plant_id, line_id, iso_year, iso_week). Scrap is bucketed by the
ISO week of the UTC scrap_ts_utc; good units by the ISO week of the local
production_day. See docs/migration/scrap_yield_weekly.md.
"""
import argparse
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.paths import abfss_uri
from mfg_lake.common.spark import get_spark
from mfg_lake.jobs.daily_production import (
    build_shift_calendar, plant_zones, stage_production_local)

REPORT = "scrap_yield_weekly"
FEEDS = ("mes.production_count", "mes.scrap_event", "mes.production_order",
         "dim.line", "dim.plant", "dim.shift_pattern", "dim.calendar")
# NOT NULL columns (legacy/sql/schema/mes.sql)
SE_NOT_NULL = ("scrap_id", "line_id", "scrap_ts_utc", "qty_units", "scrap_type")
PO_NOT_NULL = ("order_id", "line_id", "plant_id", "sku_id",
               "sched_start_utc", "sched_end_utc", "planned_qty")
OUTPUT_COLUMNS = ("plant_id", "line_id", "iso_year", "iso_week",
                  "good_units", "scrap_units", "scrap_pct")
ALLOC_WINDOW = "INTERVAL 7200 SECONDS"  # @ts -/+ 7200 (seconds) in the proc
INT_MAX = 2**31 - 1


def _scrap_events(scrap_event: DataFrame) -> DataFrame:
    return scrap_event.select(
        F.col("scrap_id").cast("int").alias("scrap_id"),
        F.trim(F.col("line_id").cast("string")).alias("line_id"),
        F.col("scrap_ts_utc").cast("timestamp").alias("scrap_ts_utc"),
        F.col("qty_units").cast("int").alias("qty_units"),
        F.col("scrap_type").cast("string").alias("scrap_type"),
    ).dropna(subset=list(SE_NOT_NULL))


def _line_dim(line: DataFrame) -> DataFrame:
    """dim.line with trimmed ids: plant_id is CHAR(5) and T-SQL compares
    ignoring trailing spaces; Spark does not."""
    return line.select(
        F.trim(F.col("line_id").cast("string")).alias("line_id"),
        F.trim(F.col("plant_id").cast("string")).alias("plant_id"),
        *[F.col(c) for c in line.columns if c not in ("line_id", "plant_id")])


def _production_orders(production_order: DataFrame) -> DataFrame:
    return production_order.select(
        F.trim(F.col("order_id").cast("string")).alias("order_id"),
        F.trim(F.col("line_id").cast("string")).alias("line_id"),
        F.trim(F.col("plant_id").cast("string")).alias("plant_id"),
        F.col("sku_id").cast("string").alias("sku_id"),
        F.col("sched_start_utc").cast("timestamp").alias("sched_start_utc"),
        F.col("sched_end_utc").cast("timestamp").alias("sched_end_utc"),
        F.col("planned_qty").cast("int").alias("planned_qty"),
    ).dropna(subset=list(PO_NOT_NULL))


def iso_year_week(col):
    """DATEPART(ISO_WEEK, col) plus the legacy ISO year:
    YEAR(DATEADD(DAY, 26 - ISO_WEEK, col)). Returns a struct column."""
    d = F.to_date(col)
    wk = F.weekofyear(d)
    return F.struct(
        F.year(F.date_add(d, 26 - wk)).alias("iso_year"),
        wk.alias("iso_week"))


def allocate_scrap_to_orders(scrap_event: DataFrame,
                             production_order: DataFrame) -> DataFrame:
    """mes.usp_allocate_scrap_to_orders -> stg.scrap_alloc rows.

    Candidates: same line, sched_start < ts+7200s, sched_end > ts-7200s and
    ts BETWEEN sched_start AND sched_end. Each candidate gets
    floor(qty * ov_secs / tot); the remainder goes to the TOP 1 order
    (ov_secs DESC, order_id). No candidate -> ('UNALLOCATED', qty).
    """
    se, po = _scrap_events(scrap_event), _production_orders(production_order)
    ts = F.col("scrap_ts_utc")
    lo, hi = ts - F.expr(ALLOC_WINDOW), ts + F.expr(ALLOC_WINDOW)
    ov_secs = (F.least(F.col("sched_end_utc"), hi).cast("long")
               - F.greatest(F.col("sched_start_utc"), lo).cast("long"))
    cand = (se.join(po, "line_id")
            .where((F.col("sched_start_utc") < hi)
                   & (F.col("sched_end_utc") > lo)
                   & ts.between(F.col("sched_start_utc"), F.col("sched_end_utc")))
            .select("scrap_id", "order_id", "qty_units", ov_secs.alias("ov_secs")))
    w_all = Window.partitionBy("scrap_id")
    cand = (cand
            .withColumn("tot", F.sum("ov_secs").over(w_all))
            .withColumn("num", F.col("qty_units").cast("long") * F.col("ov_secs")))
    # SQL Server: SUM(INT)/INT*INT arithmetic overflow raises; Spark wraps.
    if cand.filter(F.col("tot") == 0).count():
        raise ZeroDivisionError("Divide by zero error encountered.")
    bad = cand.filter((F.abs(F.col("num")) > INT_MAX)
                      | (F.abs(F.col("tot")) > INT_MAX)).count()
    if bad:
        raise ArithmeticError(f"{bad} rows overflow INT (Arithmetic overflow in legacy)")
    w_rank = w_all.orderBy(F.col("ov_secs").desc(), F.col("order_id").asc())
    alloc = (cand
             .withColumn("base", F.expr("num div tot"))
             .withColumn("rn", F.row_number().over(w_rank))
             .withColumn("qty_units",
                         F.col("base") + F.when(F.col("rn") == 1,
                                                F.col("qty_units") - F.sum("base").over(w_all))
                         .otherwise(F.lit(0)))
             .select("scrap_id", "order_id",
                     F.col("qty_units").cast("int").alias("qty_units")))
    un = (se.join(cand.select("scrap_id").distinct(), "scrap_id", "left_anti")
          .select("scrap_id", F.lit("UNALLOCATED").alias("order_id"), "qty_units"))
    return alloc.unionByName(un)


def stage_scrap_weekly(scrap_event: DataFrame, line: DataFrame) -> DataFrame:
    """mes.scrap_event JOIN dim.line, SUM(qty_units) by ISO week of the
    UTC scrap_ts_utc."""
    ln = _line_dim(line).select("line_id", "plant_id")
    s = (_scrap_events(scrap_event).join(ln, "line_id")
         .select("plant_id", "line_id",
                 iso_year_week(F.col("scrap_ts_utc")).alias("w"), "qty_units"))
    return (s.groupBy("plant_id", "line_id", "w")
            .agg(F.sum("qty_units").alias("scrap_units"))
            .select("plant_id", "line_id",
                    F.col("w.iso_year").cast("int").alias("iso_year"),
                    F.col("w.iso_week").cast("int").alias("iso_week"),
                    "scrap_units"))


def stage_good_weekly(stg: DataFrame) -> DataFrame:
    """stg.production_local (no dim.sku join) -> SUM(good_units) by ISO week
    of production_day."""
    s = stg.select("plant_id", "line_id",
                   iso_year_week(F.col("production_day")).alias("w"), "good_units")
    return (s.groupBy("plant_id", "line_id", "w")
            .agg(F.sum("good_units").alias("good_units"))
            .select("plant_id", "line_id",
                    F.col("w.iso_year").cast("int").alias("iso_year"),
                    F.col("w.iso_week").cast("int").alias("iso_week"),
                    "good_units"))


KEYS = ["plant_id", "line_id", "iso_year", "iso_week"]


def build_report(good: DataFrame, scrap: DataFrame) -> DataFrame:
    """rpt.usp_rpt_scrap_yield_weekly: good LEFT JOIN scrap on the 4 keys;
    scrap-only weeks are dropped."""
    g = (good.join(scrap, KEYS, "left")
         .withColumn("scrap_units", F.coalesce(F.col("scrap_units"), F.lit(0))))
    den = F.col("good_units") + F.col("scrap_units")
    # ROUND(CAST(scrap AS DECIMAL(18,4)) / NULLIF(good+scrap,0) * 100, 2)
    ratio = (F.col("scrap_units").cast("decimal(18,4)")
             / F.when(den != 0, den).cast("decimal(10,0)")
             * F.lit(100).cast("decimal(10,0)"))
    return g.select(
        F.col("plant_id").cast("string").alias("plant_id"),
        F.col("line_id").cast("string").alias("line_id"),
        F.col("iso_year").cast("int").alias("iso_year"),
        F.col("iso_week").cast("int").alias("iso_week"),
        F.col("good_units").cast("int").alias("good_units"),
        F.col("scrap_units").cast("int").alias("scrap_units"),
        F.round(ratio, 2).cast("decimal(9,2)").alias("scrap_pct"),
        F.col("good_units").alias("_g"), F.col("scrap_units").alias("_s"),
        den.alias("_den"))


def _check_int_overflow(df: DataFrame) -> None:
    """SQL Server SUM(INT)/INT+INT raises on overflow; Spark sums as BIGINT."""
    bad = df.filter((F.abs("_g") > INT_MAX) | (F.abs("_s") > INT_MAX)
                    | (F.abs("_den") > INT_MAX)).count()
    if bad:
        raise ArithmeticError(f"{bad} groups overflow INT (Arithmetic overflow in legacy)")


def transform(feeds: dict) -> DataFrame:
    """Raw feeds (keyed by schema.table) -> rpt.scrap_yield_weekly rows."""
    line = _line_dim(feeds["dim.line"])
    sc = build_shift_calendar(feeds["dim.plant"], feeds["dim.shift_pattern"],
                              feeds["dim.calendar"])
    stg = stage_production_local(feeds["mes.production_count"], line,
                                 feeds["dim.plant"], sc)
    rpt = build_report(stage_good_weekly(stg),
                       stage_scrap_weekly(feeds["mes.scrap_event"], line))
    _check_int_overflow(rpt)
    return rpt.select(*OUTPUT_COLUMNS)


def run(ns: str, as_of_utc: str, spark: SparkSession = None):
    spark = spark or get_spark(f"mfg_lake.{REPORT}")
    datetime.strptime(as_of_utc, "%Y-%m-%d %H:%M:%S")  # format check only
    feeds = {t: read_raw(spark, t) for t in FEEDS}
    se, po = feeds["mes.scrap_event"], feeds["mes.production_order"]
    n_se = se.count()
    n_po = po.count()
    se_ok = _scrap_events(se)
    po_ok = _production_orders(po)
    n_se_ok = se_ok.count()
    n_po_ok = po_ok.count()
    line = _line_dim(feeds["dim.line"])
    n_unk = se_ok.join(line.select("line_id").distinct(), "line_id", "left_anti").count()

    sc = build_shift_calendar(feeds["dim.plant"], feeds["dim.shift_pattern"],
                              feeds["dim.calendar"])
    stg = stage_production_local(feeds["mes.production_count"], line,
                                 feeds["dim.plant"], sc).cache()
    good = stage_good_weekly(stg).cache()
    scrap = stage_scrap_weekly(se, line).cache()
    dropped = scrap.join(good, KEYS, "left_anti")
    n_drop_g = dropped.count()
    n_drop_u = dropped.agg(F.sum("scrap_units")).collect()[0][0] or 0
    alloc = allocate_scrap_to_orders(se, po).cache()
    n_alloc = alloc.count()
    n_unalloc = alloc.filter(F.col("order_id") == "UNALLOCATED").count()
    rpt = build_report(good, scrap).cache()
    _check_int_overflow(rpt)
    out = rpt.select(*OUTPUT_COLUMNS).orderBy(*KEYS)
    dest = write_curated(out, REPORT, ns)
    print(f"[{REPORT}] as_of_utc={as_of_utc} (not applied, as legacy) "
          f"production_count={feeds['mes.production_count'].count()} "
          f"scrap_event={n_se} dropped_not_null={n_se - n_se_ok} "
          f"production_order={n_po} dropped_not_null={n_po - n_po_ok} "
          f"scrap_unknown_line_dropped={n_unk} "
          f"shift_calendar={sc.count()} staged_production={stg.count()} "
          f"alloc_rows={n_alloc} unallocated_events={n_unalloc} "
          f"scrap_groups_dropped_by_left_join={n_drop_g} units={n_drop_u} "
          f"rows={rpt.count()}")
    print(f"[{REPORT}] wrote {abfss_uri(REPORT)} -> {dest}")
    for df in (stg, good, scrap, alloc, rpt):
        df.unpersist()
    return dest


def _as_of(v: str) -> str:
    try:
        datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--as-of-utc must be 'YYYY-MM-DD HH:MM:SS', got {v!r}")
    return v


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ns", required=True, help="lake namespace (local: out/<ns>)")
    ap.add_argument("--as-of-utc", required=True, type=_as_of,
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
