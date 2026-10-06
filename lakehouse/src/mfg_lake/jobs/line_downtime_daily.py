"""PL_Line_Downtime -> rpt.line_downtime_daily (PySpark conversion).

Mirrors the legacy chain exactly:
  mes.usp_stg_downtime_local      (UTC -> plant local, cap open at AsOf)
  mes.usp_split_downtime_by_shift (overlap vs dim.shift_calendar bounds,
                                   shifted to local wall time)
  rpt.usp_rpt_line_downtime_daily (DATEDIFF(MINUTE) minute-boundary count)

Usage: python -m mfg_lake.jobs.line_downtime_daily --ns <ns> --as-of-utc <cutoff>
"""
import argparse
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw, write_curated
from mfg_lake.common.spark import get_spark
from mfg_lake.common.tz import local_to_utc, windows_to_iana

REPORT = "line_downtime_daily"


def _hm(v):
    """Parse HH:MM whether it came back as str or a Timestamp/time."""
    if hasattr(v, "hour"):
        return v.hour, v.minute
    h, m = str(v).split(":")[:2]
    return int(h), int(m)


def _shift_calendar_pd(pd_frames):
    """dim.shift_calendar equivalent: shift bounds in UTC per plant/day.

    Local bounds come from dim.shift_pattern; local->UTC must follow the
    AT TIME ZONE rule (pre-transition offset on ambiguous/gap wall times),
    so it is computed in the driver with zoneinfo.
    """
    import pandas as pd
    plant = pd_frames["dim.plant"].set_index("plant_id")
    pat = pd_frames["dim.shift_pattern"]
    rows = []
    for d in pd_frames["dim.calendar"]["calendar_date"]:
        day = datetime.combine(d, datetime.min.time()) if not isinstance(d, str) \
            else datetime.strptime(d, "%Y-%m-%d")
        dstr = d if isinstance(d, str) else d.isoformat()
        for _, s in pat.iterrows():
            tz = plant.loc[s["plant_id"], "tz_name"]
            h0, m0 = _hm(s["local_start"])
            h1, m1 = _hm(s["local_end"])
            ls = day + timedelta(hours=h0, minutes=m0)
            le = day + timedelta(days=int(s["end_next_day"]),
                                 hours=h1, minutes=m1)
            rows.append((s["plant_id"], s["shift_code"], dstr,
                         local_to_utc(ls, tz), local_to_utc(le, tz)))
    return pd.DataFrame(rows, columns=["plant_id", "shift_code",
                                       "production_day", "start_utc", "end_utc"])


def build_report(spark, frames, pd_frames, as_of_utc):
    as_of = datetime.strptime(as_of_utc, "%Y-%m-%d %H:%M:%S")
    sc = spark.createDataFrame(_shift_calendar_pd(pd_frames))
    tzmap = spark.createDataFrame(
        [(w, windows_to_iana(w))
         for w in pd_frames["dim.plant"].tz_name.unique()],
        ["tz_name", "iana"])

    plants = frames["dim.plant"].select("plant_id", "tz_name")
    sc = (sc.join(plants, "plant_id")
            .join(tzmap, "tz_name")
            # CAST(sc.*_utc AT TIME ZONE 'UTC' AT TIME ZONE tz AS datetime2)
            .withColumn("start_local", F.from_utc_timestamp("start_utc", F.col("iana")))
            .withColumn("end_local", F.from_utc_timestamp("end_utc", F.col("iana")))
            .drop("tz_name", "iana"))

    ev = (frames["mes.downtime_event"]
          .join(frames["dim.line"].select("line_id", "plant_id"), "line_id")
          .join(plants, "plant_id")
          .join(tzmap, "tz_name")
          .filter(F.col("start_utc").cast("timestamp") < F.lit(as_of))
          .withColumn("start_utc", F.col("start_utc").cast("timestamp"))
          .withColumn("end_utc",
                      F.coalesce(F.col("end_utc").cast("timestamp"),
                                 F.lit(as_of)))
          .withColumn("start_local", F.from_utc_timestamp("start_utc", F.col("iana")))
          .withColumn("end_local", F.from_utc_timestamp("end_utc", F.col("iana"))))

    seg = (ev.alias("e")
           .join(sc.alias("s"),
                 (F.col("e.plant_id") == F.col("s.plant_id"))
                 & (F.col("s.start_local") < F.col("e.end_local"))
                 & (F.col("s.end_local") > F.col("e.start_local")))
           .select(
               "e.event_id", "e.plant_id", "e.line_id", "e.reason_code",
               F.col("e.planned_flag").cast("int").alias("planned_flag"),
               F.to_date("s.production_day").alias("production_day"),
               "s.shift_code",
               F.greatest("e.start_local", "s.start_local").alias("seg_start"),
               F.least("e.end_local", "s.end_local").alias("seg_end")))

    # DATEDIFF(MINUTE, a, b): minute boundaries crossed
    # = trunc_to_minute(b) - trunc_to_minute(a), not elapsed/60.
    seg = seg.withColumn(
        "mins",
        ((F.unix_timestamp(F.date_trunc("minute", "seg_end"))
          - F.unix_timestamp(F.date_trunc("minute", "seg_start"))) / 60)
        .cast("int"))

    reasons = frames["dim.downtime_reason"].select("reason_code", "reason_category")
    return (seg.join(reasons, "reason_code")
              .groupBy("plant_id", "line_id", "production_day", "shift_code",
                       "reason_category", "planned_flag")
              .agg(F.countDistinct("event_id").alias("event_count"),
                   F.sum("mins").alias("downtime_minutes")))


DIM_TABLES = ["dim.line", "dim.plant", "dim.shift_pattern", "dim.calendar",
              "dim.downtime_reason"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ns", required=True)
    ap.add_argument("--as-of-utc", required=True)
    args = ap.parse_args()
    spark = get_spark(REPORT)
    frames = {t: read_raw(spark, t) for t in DIM_TABLES + ["mes.downtime_event"]}
    pd_frames = {t: frames[t].toPandas() for t in DIM_TABLES}
    out = build_report(spark, frames, pd_frames, args.as_of_utc)
    dest = write_curated(out, REPORT, args.ns)
    print(f"wrote {out.count()} rows -> {dest}")
    spark.stop()


if __name__ == "__main__":
    main()
