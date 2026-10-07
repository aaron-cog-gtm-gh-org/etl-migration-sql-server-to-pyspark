"""PL_Daily_Production conversion: SQL Server behaviours that must survive.

Written before mfg_lake.jobs.daily_production; each test pins one legacy
behaviour from mes.usp_stg_production_counts / rpt.usp_rpt_daily_production
/ dim.usp_refresh_shift_calendar / dim.ufn_production_day.
"""
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from mfg_lake.common.spark import get_spark
from mfg_lake.jobs import daily_production as job

PLANTS = [
    ("PLT01", "Neenah Tissue", "Central Standard Time", "3x8"),
    ("PLT04", "Monterrey Wipes", "Central Standard Time (Mexico)", "3x8"),
    ("PLT06", "Hull Wipes", "GMT Standard Time", "2x12"),
]
LINES = [("PLT01-L1", "PLT01", "Line 1"), ("PLT04-L1", "PLT04", "Line 1"),
         ("PLT06-L1", "PLT06", "Line 1")]
SKUS = [("SKU-A", "a", 24, 100), ("SKU-B", "b", 10, 100)]
PATTERNS = [
    ("PLT01", "S1", "06:00", "14:00", 0), ("PLT01", "S2", "14:00", "22:00", 0),
    ("PLT01", "S3", "22:00", "06:00", 1),
    ("PLT04", "S1", "06:00", "14:00", 0), ("PLT04", "S2", "14:00", "22:00", 0),
    ("PLT04", "S3", "22:00", "06:00", 1),
    ("PLT06", "D", "06:00", "18:00", 0), ("PLT06", "N", "18:00", "06:00", 1),
]
CAL_START, CAL_DAYS = date(2025, 10, 20), 28


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test_daily_production")
    yield s
    s.stop()


def _feeds(spark, counts, patterns=PATTERNS, cal_start=CAL_START, cal_days=CAL_DAYS):
    cal = [((cal_start + timedelta(days=i)).isoformat(), 0, 0, "")
           for i in range(cal_days)]
    rows = [(i + 1, *r) for i, r in enumerate(counts)]
    return {
        "dim.plant": spark.createDataFrame(
            PLANTS, "plant_id string, plant_name string, tz_name string, shift_pattern string"),
        "dim.line": spark.createDataFrame(LINES, "line_id string, plant_id string, line_name string"),
        "dim.sku": spark.createDataFrame(
            SKUS, "sku_id string, product_name string, pack_size int, ideal_units_per_min int"),
        "dim.shift_pattern": spark.createDataFrame(
            patterns, "plant_id string, shift_code string, local_start string, "
                      "local_end string, end_next_day int"),
        "dim.calendar": spark.createDataFrame(
            cal, "calendar_date string, iso_week int, day_of_week int, day_name string"),
        "mes.production_count": spark.createDataFrame(
            rows, "bucket_id int, line_id string, sku_id string, bucket_start_utc string, "
                  "bucket_end_utc string, total_units int, good_units int"),
    }


def _bucket(line, sku, start_utc, total, good, minutes=15):
    s = datetime.fromisoformat(start_utc)
    return (line, sku, s.strftime("%Y-%m-%d %H:%M:%S"),
            (s + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S"), total, good)


def _run(spark, counts, **kw):
    return {(r.line_id, r.production_day.isoformat(), r.sku_id): r
            for r in job.transform(_feeds(spark, counts, **kw)).collect()}


# ---------------------------------------------------------------- report math

def test_cases_is_integer_division_of_good_units(spark):
    # 47 good / pack 24 = 1.958 -> SQL INT division -> 1 (not 2, not 1.958)
    out = _run(spark, [_bucket("PLT01-L1", "SKU-A", "2025-10-21 15:00:00", 60, 47)])
    r = out[("PLT01-L1", "2025-10-21", "SKU-A")]
    assert (r.total_units, r.good_units, r.cases) == (60, 47, 1)


def test_cases_uses_summed_good_units_not_per_bucket(spark):
    # 2 buckets of 23 good: per-bucket division would give 0+0, SUM first gives 46/24=1
    out = _run(spark, [_bucket("PLT01-L1", "SKU-A", "2025-10-21 15:00:00", 30, 23),
                       _bucket("PLT01-L1", "SKU-A", "2025-10-21 15:15:00", 30, 23)])
    assert out[("PLT01-L1", "2025-10-21", "SKU-A")].cases == 1


def test_yield_null_when_total_zero(spark):
    out = _run(spark, [_bucket("PLT01-L1", "SKU-A", "2025-10-21 15:00:00", 0, 0)])
    r = out[("PLT01-L1", "2025-10-21", "SKU-A")]
    assert r.total_units == 0 and r.cases == 0 and r.yield_pct is None


@pytest.mark.parametrize("total,good,expected", [
    (160, 157, Decimal("98.13")),   # 98.125 exact tie -> away from zero (banker's: 98.12)
    (8, 1, Decimal("12.50")),       # 12.5 exact
    (3, 2, Decimal("66.67")),
    (6, 1, Decimal("16.67")),
    (2000, 1999, Decimal("99.95")),
    (200000, 199999, Decimal("100.00")),  # 99.9995 rounds up to 100.00
])
def test_yield_round_half_away_from_zero(spark, total, good, expected):
    out = _run(spark, [_bucket("PLT01-L1", "SKU-A", "2025-10-21 15:00:00", total, good)])
    assert out[("PLT01-L1", "2025-10-21", "SKU-A")].yield_pct == expected


def test_output_schema_matches_rpt_daily_production(spark):
    df = job.transform(_feeds(spark, [_bucket("PLT01-L1", "SKU-A", "2025-10-21 15:00:00", 10, 9)]))
    assert [(f.name, f.dataType) for f in df.schema.fields] == [
        ("plant_id", T.StringType()), ("line_id", T.StringType()),
        ("production_day", T.DateType()), ("sku_id", T.StringType()),
        ("total_units", T.IntegerType()), ("good_units", T.IntegerType()),
        ("cases", T.IntegerType()), ("yield_pct", T.DecimalType(9, 2)),
    ]


# ------------------------------------------------------- staging / filtering

def test_unknown_sku_is_dropped(spark):
    out = _run(spark, [_bucket("PLT01-L1", "SKU-ZZ", "2025-10-21 15:00:00", 10, 9),
                       _bucket("PLT01-L1", "SKU-A", "2025-10-21 15:00:00", 10, 9)])
    assert set(out) == {("PLT01-L1", "2025-10-21", "SKU-A")}


def test_unknown_line_is_dropped(spark):
    out = _run(spark, [_bucket("PLT99-L1", "SKU-A", "2025-10-21 15:00:00", 10, 9)])
    assert out == {}


def test_bucket_outside_shift_calendar_window_is_dropped(spark):
    # calendar starts 2025-10-20 -> first shift starts 06:00 CDT = 11:00Z
    out = _run(spark, [_bucket("PLT01-L1", "SKU-A", "2025-10-20 10:45:00", 10, 9),
                       _bucket("PLT01-L1", "SKU-A", "2025-10-20 11:00:00", 10, 9)])
    assert out[("PLT01-L1", "2025-10-20", "SKU-A")].total_units == 10
    assert len(out) == 1


def test_duplicate_buckets_are_summed_like_legacy(spark):
    # mes.production_count has no PK; legacy SUM double counts duplicates
    b = _bucket("PLT01-L1", "SKU-A", "2025-10-21 15:00:00", 10, 9)
    out = _run(spark, [b, b])
    assert out[("PLT01-L1", "2025-10-21", "SKU-A")].total_units == 20


def test_rows_violating_not_null_contract_are_dropped(spark):
    out = _run(spark, [("PLT01-L1", "SKU-A", "2025-10-21 15:00:00", "2025-10-21 15:15:00", None, 5),
                       _bucket("PLT01-L1", "SKU-A", "2025-10-21 15:15:00", 10, 9)])
    r = out[("PLT01-L1", "2025-10-21", "SKU-A")]
    assert (r.total_units, r.good_units) == (10, 9)


# --------------------------------------------------------- production day

def test_production_day_anchored_at_0600_local(spark):
    # PLT01 2025-10-21 is CDT (UTC-5): 05:45 local = 10:45Z -> day 10-20; 06:00 local -> 10-21
    out = _run(spark, [_bucket("PLT01-L1", "SKU-A", "2025-10-21 10:45:00", 10, 9),
                       _bucket("PLT01-L1", "SKU-B", "2025-10-21 11:00:00", 10, 9)])
    assert ("PLT01-L1", "2025-10-20", "SKU-A") in out
    assert ("PLT01-L1", "2025-10-21", "SKU-B") in out


def test_bucket_straddling_0600_attributed_by_start(spark):
    # 05:50 -> 06:05 local: whole bucket belongs to the previous production day
    out = _run(spark, [_bucket("PLT01-L1", "SKU-A", "2025-10-21 10:50:00", 10, 9, minutes=15)])
    assert set(out) == {("PLT01-L1", "2025-10-20", "SKU-A")}


def test_bucket_straddling_shift_boundary_kept_once(spark):
    # 13:55 -> 14:10 local spans S1/S2; range join on start puts it in S1 exactly once
    out = _run(spark, [_bucket("PLT01-L1", "SKU-A", "2025-10-21 18:55:00", 10, 9)])
    assert out[("PLT01-L1", "2025-10-21", "SKU-A")].total_units == 10


def test_production_day_uses_dst_offset_of_bucket(spark):
    # London: 2025-10-25 BST (UTC+1) 05:30Z = 06:30 local -> day 10-25;
    # 2025-10-27 GMT 05:30Z = 05:30 local -> day 10-26
    out = _run(spark, [_bucket("PLT06-L1", "SKU-A", "2025-10-25 05:30:00", 10, 9),
                       _bucket("PLT06-L1", "SKU-B", "2025-10-27 05:30:00", 10, 9)])
    assert ("PLT06-L1", "2025-10-25", "SKU-A") in out
    assert ("PLT06-L1", "2025-10-26", "SKU-B") in out


def test_fall_back_hour_buckets_land_on_same_production_day(spark):
    # Chicago 2025-11-02: 01:30 CDT (06:30Z) and 01:30 CST (07:30Z) both -> day 11-01
    out = _run(spark, [_bucket("PLT01-L1", "SKU-A", "2025-11-02 06:30:00", 10, 9),
                       _bucket("PLT01-L1", "SKU-A", "2025-11-02 07:30:00", 10, 9)])
    assert out[("PLT01-L1", "2025-11-01", "SKU-A")].total_units == 20


def test_mexico_has_no_dst(spark):
    # 2025-11-03 11:45Z = 05:45 CST(Mexico, UTC-6) -> day 11-02 (would be 11-03 if DST applied)
    out = _run(spark, [_bucket("PLT04-L1", "SKU-A", "2025-11-03 11:45:00", 10, 9)])
    assert set(out) == {("PLT04-L1", "2025-11-02", "SKU-A")}


# ----------------------------------------------------------- shift calendar

def _shift_cal(spark, patterns=PATTERNS, cal_start=CAL_START, cal_days=CAL_DAYS):
    f = _feeds(spark, [], patterns=patterns, cal_start=cal_start, cal_days=cal_days)
    rows = job.build_shift_calendar(f["dim.plant"], f["dim.shift_pattern"],
                                    f["dim.calendar"]).collect()
    return {(r.plant_id, r.shift_code, r.production_day.isoformat()): (r.start_utc, r.end_utc)
            for r in rows}


def test_shift_calendar_covers_full_calendar_window(spark):
    sc = _shift_cal(spark)
    assert len(sc) == len(PATTERNS) * CAL_DAYS


def test_shift_calendar_is_contiguous_per_plant(spark):
    sc = _shift_cal(spark)
    for plant in {k[0] for k in sc}:
        spans = sorted(v for k, v in sc.items() if k[0] == plant)
        for (s0, e0), (s1, _) in zip(spans, spans[1:]):
            assert e0 == s1, (plant, e0, s1)


def test_shift_calendar_from_inferred_timestamp_times(spark):
    # read_raw(inferSchema) turns '06:00' into a timestamp on today's date
    f = _feeds(spark, [])
    sp = f["dim.shift_pattern"].selectExpr(
        "plant_id", "shift_code", "end_next_day",
        "to_timestamp(concat('2030-01-01 ', local_start)) AS local_start",
        "to_timestamp(concat('2030-01-01 ', local_end)) AS local_end")
    rows = job.build_shift_calendar(f["dim.plant"], sp, f["dim.calendar"]).collect()
    assert len(rows) == len(PATTERNS) * CAL_DAYS
    r = next(r for r in rows if (r.plant_id, r.shift_code, r.production_day.isoformat())
             == ("PLT01", "S1", "2025-10-20"))
    assert (r.start_utc, r.end_utc) == (datetime(2025, 10, 20, 11), datetime(2025, 10, 20, 19))


def test_fall_back_night_shift_is_one_hour_longer(spark):
    sc = _shift_cal(spark)
    s, e = sc[("PLT01", "S3", "2025-11-01")]
    assert (s, e) == (datetime(2025, 11, 2, 3), datetime(2025, 11, 2, 12))   # 9h
    s, e = sc[("PLT06", "N", "2025-10-25")]
    assert (s, e) == (datetime(2025, 10, 25, 17), datetime(2025, 10, 26, 6))  # 13h
    s, e = sc[("PLT04", "S3", "2025-11-01")]
    assert e - s == timedelta(hours=8)  # Mexico: no DST


def test_fall_back_24h_shift_is_25h(spark):
    pats = [("PLT01", "X", "06:00", "06:00", 1)]
    sc = _shift_cal(spark, patterns=pats, cal_start=date(2025, 11, 1), cal_days=2)
    s, e = sc[("PLT01", "X", "2025-11-01")]
    assert e - s == timedelta(hours=25)
    s, e = sc[("PLT01", "X", "2025-11-02")]
    assert e - s == timedelta(hours=24)


def test_spring_forward_gap_shifts_forward(spark):
    # 2025-03-09 02:30 does not exist in Chicago; AT TIME ZONE moves it forward
    # by the gap -> 03:30 CDT = 08:30Z
    pats = [("PLT01", "G", "02:30", "04:00", 0)]
    sc = _shift_cal(spark, patterns=pats, cal_start=date(2025, 3, 9), cal_days=1)
    assert sc[("PLT01", "G", "2025-03-09")] == (datetime(2025, 3, 9, 8, 30),
                                               datetime(2025, 3, 9, 9, 0))


def test_fall_back_ambiguous_time_uses_pre_transition_offset(spark):
    # 2025-11-02 01:30 occurs twice in Chicago; AT TIME ZONE takes the first (CDT)
    pats = [("PLT01", "A", "01:30", "03:00", 0)]
    sc = _shift_cal(spark, patterns=pats, cal_start=date(2025, 11, 2), cal_days=1)
    assert sc[("PLT01", "A", "2025-11-02")] == (datetime(2025, 11, 2, 6, 30),
                                               datetime(2025, 11, 2, 9, 0))


# ------------------------------------------------------------------- CLI

def test_cli_accepts_ns_and_as_of_utc():
    a = job.parse_args(["--ns", "x", "--as-of-utc", "2025-11-17 00:00:00"])
    assert (a.ns, a.as_of_utc) == ("x", "2025-11-17 00:00:00")
