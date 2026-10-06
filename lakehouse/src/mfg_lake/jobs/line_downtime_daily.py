"""PL_Line_Downtime -> rpt.line_downtime_daily (PySpark conversion).

Straight port of the legacy proc chain:
  mes.usp_stg_downtime_local -> mes.usp_split_downtime_by_shift
  -> rpt.usp_rpt_line_downtime_daily

Usage: python -m mfg_lake.jobs.line_downtime_daily --ns <ns> --as-of-utc <cutoff>
"""
import argparse

from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.spark import get_spark

REPORT = "line_downtime_daily"

# shift windows (hour-of-day -> shift_code) per pattern
PATTERN_3X8 = [(6, "S1"), (14, "S2"), (22, "S3")]
PATTERN_2X12 = [(6, "D"), (18, "N")]


def shift_code_for_hour(hour: int, pattern: str) -> str:
    table = PATTERN_3X8 if pattern == "3x8" else PATTERN_2X12
    codes = [c for h, c in table if hour >= h] or [table[-1][1]]
    return codes[-1]


def build_report(spark, as_of_utc):
    events = read_raw(spark, "mes.downtime_event")
    lines = read_raw(spark, "dim.line")
    plants = read_raw(spark, "dim.plant")
    reasons = read_raw(spark, "dim.downtime_reason")

    df = (events.join(lines, "line_id")
                .join(plants, "plant_id")
                .join(reasons, "reason_code"))

    cap = F.lit(as_of_utc).cast("timestamp")
    df = (df.filter(F.col("start_utc").cast("timestamp") < cap)
            .withColumn("start_ts", F.col("start_utc").cast("timestamp"))
            .withColumn("end_ts",
                        F.coalesce(F.col("end_utc").cast("timestamp"), cap))
            # production day: 06:00 anchor
            .withColumn("production_day",
                        F.to_date(F.col("start_ts") - F.expr("INTERVAL 6 HOURS")))
            # minutes: elapsed time
            .withColumn("mins",
                        (F.unix_timestamp("end_ts")
                         - F.unix_timestamp("start_ts")) / 60.0))

    udf_shift = F.udf(shift_code_for_hour)
    df = (df.withColumn("shift_code",
                        udf_shift(F.hour("start_ts"), F.col("shift_pattern")))
            .withColumn("mins", F.col("mins").cast("int")))

    return (df.groupBy("plant_id", "line_id", "production_day", "shift_code",
                       "reason_category", "planned_flag")
              .agg(F.countDistinct("event_id").alias("event_count"),
                   F.sum("mins").cast("int").alias("downtime_minutes")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ns", required=True)
    ap.add_argument("--as-of-utc", required=True)
    args = ap.parse_args()
    spark = get_spark(REPORT)
    out = build_report(spark, args.as_of_utc)
    dest = write_curated(out, REPORT, args.ns)
    print(f"wrote {out.count()} rows -> {dest}")
    spark.stop()


if __name__ == "__main__":
    main()
