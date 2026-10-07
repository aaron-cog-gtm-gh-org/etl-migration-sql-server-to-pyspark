"""PL_Line_Downtime conversion: SQL Server behaviours that must survive.

Written before mfg_lake.jobs.line_downtime_daily; each test pins one legacy
behaviour from mes.usp_stg_downtime_local / mes.usp_split_downtime_by_shift
/ rpt.usp_rpt_line_downtime_daily.
"""
import inspect
from datetime import date, datetime, timedelta

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from mfg_lake.common.spark import get_spark
from mfg_lake.jobs import daily_production
from mfg_lake.jobs import line_downtime_daily as job

PLANTS = [
    ("PLT01", "Neenah Tissue", "Central Standard Time", "3x8"),
    ("PLT04", "Monterrey Wipes", "Central Standard Time (Mexico)", "3x8"),
    ("PLT06", "Hull Wipes", "GMT Standard Time", "2x12"),
]
LINES = [("PLT01-L1", "PLT01", "Line 1"), ("PLT04-L1", "PLT04", "Line 1"),
         ("PLT06-L1", "PLT06", "Line 1")]
REASONS = [("R-JAM", "Sheet jam / web break", "Mechanical", 0),
           ("R-CO", "Changeover", "Changeover", 1),
           ("R-PM", "Planned maintenance", "Planned", 1)]
PATTERNS = [
    ("PLT01", "S1", "06:00", "14:00", 0), ("PLT01", "S2", "14:00", "22:00", 0),
    ("PLT01", "S3", "22:00", "06:00", 1),
    ("PLT04", "S1", "06:00", "14:00", 0), ("PLT04", "S2", "14:00", "22:00", 0),
    ("PLT04", "S3", "22:00", "06:00", 1),
    ("PLT06", "D", "06:00", "18:00", 0), ("PLT06", "N", "18:00", "06:00", 1),
]
CAL_START, CAL_DAYS = date(2025, 10, 20), 28
AS_OF = "2025-11-17 00:00:00"


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test_line_downtime_daily")
    yield s
    s.stop()


def _ev(line, start_utc, end_utc, reason="R-JAM", planned=0):
    return (line, start_utc, end_utc, reason, planned)


def _feeds(spark, events, patterns=PATTERNS, cal_start=CAL_START, cal_days=CAL_DAYS,
           plants=PLANTS, lines=LINES, reasons=REASONS):
    cal = [((cal_start + timedelta(days=i)).isoformat(), 0, 0, "")
           for i in range(cal_days)]
    rows = [((i + 1,) + tuple(e)) if len(e) == 5 else tuple(e)
            for i, e in enumerate(events)]
    return {
        "dim.plant": spark.createDataFrame(
            plants, "plant_id string, plant_name string, tz_name string, shift_pattern string"),
        "dim.line": spark.createDataFrame(
            lines, "line_id string, plant_id string, line_name string"),
        "dim.downtime_reason": spark.createDataFrame(
            reasons, "reason_code string, reason_desc string, reason_category string, "
                     "planned_default int"),
        "dim.shift_pattern": spark.createDataFrame(
            patterns, "plant_id string, shift_code string, local_start string, "
                      "local_end string, end_next_day int"),
        "dim.calendar": spark.createDataFrame(
            cal, "calendar_date string, iso_week int, day_of_week int, day_name string"),
        "mes.downtime_event": spark.createDataFrame(
            rows, "event_id int, line_id string, start_utc string, end_utc string, "
                  "reason_code string, planned_flag int"),
    }


def _shift_calendar(feeds):
    return daily_production.build_shift_calendar(
        feeds["dim.plant"], feeds["dim.shift_pattern"], feeds["dim.calendar"])


def _stg(feeds, as_of=AS_OF):
    return job.stage_downtime_local(feeds["mes.downtime_event"], feeds["dim.line"],
                                    feeds["dim.plant"], job.parse_as_of_utc(as_of))


def _seg(feeds, as_of=AS_OF):
    return job.split_downtime_by_shift(_stg(feeds, as_of), feeds["dim.plant"],
                                       _shift_calendar(feeds))


def _run(spark, events, as_of=AS_OF, **kw):
    out = job.transform(_feeds(spark, events, **kw), as_of)
    return {(r.line_id, r.production_day.isoformat(), r.shift_code,
             r.reason_category, r.planned_flag): r for r in out.collect()}


K = "PLT01-L1", "Mechanical"   # common key prefix pieces


def test_constants_pin_the_interface():
    assert job.REPORT == "line_downtime_daily"
    assert job.FEEDS == ("mes.downtime_event", "dim.line", "dim.plant",
                         "dim.downtime_reason", "dim.shift_pattern", "dim.calendar")
    assert job.NOT_NULL == ("event_id", "line_id", "start_utc", "reason_code",
                            "planned_flag")
    assert job.OUTPUT_COLUMNS == ("plant_id", "line_id", "production_day",
                                  "shift_code", "reason_category", "planned_flag",
                                  "event_count", "downtime_minutes")
    assert job.STG_DOWNTIME_LOCAL_COLUMNS == (
        "event_id", "plant_id", "line_id", "reason_code", "planned_flag",
        "start_utc", "end_utc", "start_local", "end_local")
    assert job.STG_DOWNTIME_SHIFT_SEG_COLUMNS == (
        "event_id", "plant_id", "line_id", "reason_code", "planned_flag",
        "production_day", "shift_code", "seg_start_local", "seg_end_local")


# ------------------------------------------------------------- DATEDIFF math

def test_datediff_minute_counts_minute_boundaries(spark):
    # T-SQL DATEDIFF(MINUTE): minute boundaries crossed, not elapsed/60
    rows = [("2025-10-20 10:00:59", "2025-10-20 10:01:00", 1),
            ("2025-10-20 10:00:00", "2025-10-20 10:00:59", 0),
            ("2025-10-20 10:00:30", "2025-10-20 10:02:29", 2),
            ("2025-10-20 10:05:00", "2025-10-20 10:01:30", -4)]
    df = spark.createDataFrame(rows, "s string, e string, expected int")
    got = df.select("expected",
                    job.datediff_minute(F.col("s").cast("timestamp"),
                                        F.col("e").cast("timestamp")).alias("m")
                    ).collect()
    assert [r.m for r in got] == [r.expected for r in got]


# ---------------------------------------------------------- shift splitting

def test_event_splits_at_shift_boundary(spark):
    # local 13:30 -> 14:45 CDT on 2025-10-23 spans S1/S2
    out = _run(spark, [_ev("PLT01-L1", "2025-10-23 18:30:00", "2025-10-23 19:45:00")])
    assert set(out) == {("PLT01-L1", "2025-10-23", "S1", "Mechanical", False),
                        ("PLT01-L1", "2025-10-23", "S2", "Mechanical", False)}
    s1 = out[("PLT01-L1", "2025-10-23", "S1", "Mechanical", False)]
    s2 = out[("PLT01-L1", "2025-10-23", "S2", "Mechanical", False)]
    assert (s1.event_count, s1.downtime_minutes) == (1, 30)
    assert (s2.event_count, s2.downtime_minutes) == (1, 45)


def test_event_crossing_midnight_stays_in_one_shift(spark):
    # local 23:40 10-25 -> 00:35 10-26 (UTC 04:40 -> 05:35 on 10-26, CDT)
    out = _run(spark, [_ev("PLT01-L1", "2025-10-26 04:40:00", "2025-10-26 05:35:00")])
    r = out[("PLT01-L1", "2025-10-25", "S3", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 55)
    assert len(out) == 1


def test_production_day_anchored_by_shift_calendar(spark):
    # local 04:00 -> 05:00 on 10-26 is S3 of production day 10-25
    out = _run(spark, [_ev("PLT01-L1", "2025-10-26 09:00:00", "2025-10-26 10:00:00")])
    r = out[("PLT01-L1", "2025-10-25", "S3", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 60)
    assert len(out) == 1


def test_event_straddling_0600_splits_days(spark):
    # local 05:30 -> 06:30 on 10-26: 30 min to S3/day-25, 30 to S1/day-26
    out = _run(spark, [_ev("PLT01-L1", "2025-10-26 10:30:00", "2025-10-26 11:30:00")])
    assert set(out) == {("PLT01-L1", "2025-10-25", "S3", "Mechanical", False),
                        ("PLT01-L1", "2025-10-26", "S1", "Mechanical", False)}
    for k in out:
        assert out[k].downtime_minutes == 30


def test_event_before_calendar_window_gets_no_segment(spark):
    # local 04:00 -> 05:00 on 10-20 ends before the first shift of day 10-20
    out = _run(spark, [_ev("PLT01-L1", "2025-10-20 09:00:00", "2025-10-20 10:00:00")])
    assert out == {}


# -------------------------------------------------------------------- DST

def test_fall_back_datediff_on_local_values(spark):
    # UTC 05:30 -> 08:30 on 2025-11-02: local 00:30 CDT -> 02:30 CST.
    # 3h elapsed but DATEDIFF on the local values counts 2h.
    out = _run(spark, [_ev("PLT01-L1", "2025-11-02 05:30:00", "2025-11-02 08:30:00")])
    r = out[("PLT01-L1", "2025-11-01", "S3", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 120)
    assert len(out) == 1


def test_fall_back_whole_night_shift_reports_480_not_540(spark):
    # UTC 03:00 -> 12:00 on 2025-11-02: local 22:00 CDT -> 06:00 CST,
    # 9h elapsed, 8 clock hours -> 480 minutes.
    out = _run(spark, [_ev("PLT01-L1", "2025-11-02 03:00:00", "2025-11-02 12:00:00")])
    r = out[("PLT01-L1", "2025-11-01", "S3", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 480)
    assert len(out) == 1


def test_event_inside_repeated_fall_back_hour_has_negative_minutes(spark):
    # UTC 06:50 -> 07:10 on 2025-11-02: local 01:50 CDT -> 01:10 CST.
    # Legacy joins in local space and keeps the row; DATEDIFF is negative.
    out = _run(spark, [_ev("PLT01-L1", "2025-11-02 06:50:00", "2025-11-02 07:10:00")])
    r = out[("PLT01-L1", "2025-11-01", "S3", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, -40)


def test_spring_forward_datediff_on_local_values(spark):
    # 2026-03-08 UTC 07:30 -> 08:30: local 01:30 CST -> 03:30 CDT.
    # 1h elapsed, 2 clock hours -> 120 minutes.
    out = _run(spark, [_ev("PLT01-L1", "2026-03-08 07:30:00", "2026-03-08 08:30:00")],
               as_of="2026-03-20 00:00:00", cal_start=date(2026, 3, 2), cal_days=14)
    r = out[("PLT01-L1", "2026-03-07", "S3", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 120)
    assert len(out) == 1


def test_spring_forward_whole_night_shift_reports_480(spark):
    # UTC 04:00 -> 11:00 on 2026-03-08: local 22:00 CST -> 06:00 CDT,
    # 7h elapsed but 8 clock hours.
    out = _run(spark, [_ev("PLT01-L1", "2026-03-08 04:00:00", "2026-03-08 11:00:00")],
               as_of="2026-03-20 00:00:00", cal_start=date(2026, 3, 2), cal_days=14)
    r = out[("PLT01-L1", "2026-03-07", "S3", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 480)
    assert len(out) == 1


def test_mexico_has_no_dst(spark):
    # Central Standard Time (Mexico) is fixed UTC-6: local 00:30 -> 02:30
    # on 2025-11-02 is a plain 2h night-shift event.
    out = _run(spark, [_ev("PLT04-L1", "2025-11-02 06:30:00", "2025-11-02 08:30:00")])
    r = out[("PLT04-L1", "2025-11-01", "S3", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 120)
    assert len(out) == 1


def test_gmt_standard_time_is_europe_london(spark):
    # UTC 17:30 -> 18:30 on 2025-10-20 is 18:30 -> 19:30 BST: inside N.
    # A UTC-as-local mapping would wrongly split it D/N at 18:00.
    out = _run(spark, [_ev("PLT06-L1", "2025-10-20 17:30:00", "2025-10-20 18:30:00")])
    r = out[("PLT06-L1", "2025-10-20", "N", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 60)
    assert len(out) == 1


def test_unmapped_tz_name_raises_keyerror(spark):
    plants = [("PLT01", "Neenah Tissue", "Vulcan Standard Time", "3x8")]
    with pytest.raises(KeyError):
        _run(spark, [_ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00")],
             plants=plants)


# ------------------------------------------------------------------ AsOfUtc

def test_event_starting_at_as_of_is_excluded(spark):
    out = _run(spark, [_ev("PLT01-L1", AS_OF, "2025-11-17 01:00:00")])
    assert out == {}


def test_event_starting_one_second_before_as_of_is_included(spark):
    # local 17:59:59 -> 18:00 CST on 11-16: S2 of day 11-16, 1 minute
    out = _run(spark, [_ev("PLT01-L1", "2025-11-16 23:59:59", "2025-11-17 00:00:00",
                           "R-PM", 1)])
    r = out[("PLT01-L1", "2025-11-16", "S2", "Planned", True)]
    assert (r.event_count, r.downtime_minutes) == (1, 1)


def test_open_event_is_capped_at_as_of(spark):
    # start 22:00Z 11-16 = 16:00 CST; capped at as_of -> local end 18:00 -> 120
    feeds = _feeds(spark, [_ev("PLT01-L1", "2025-11-16 22:00:00", None)])
    stg = _stg(feeds)
    row = stg.collect()[0]
    assert row.end_utc == datetime(2025, 11, 17, 0, 0, 0)
    out = {(r.line_id, r.production_day.isoformat(), r.shift_code): r
           for r in job.build_report(_seg(feeds), feeds["dim.downtime_reason"])
           .collect()}
    r = out[("PLT01-L1", "2025-11-16", "S2")]
    assert (r.event_count, r.downtime_minutes) == (1, 120)


def test_closed_event_past_as_of_keeps_its_real_end(spark):
    # ISNULL caps NULLs only: 23:00Z 11-16 -> 02:00Z 11-17 stays put
    # (local 17:00 -> 20:00 CST, entirely in S2).
    out = _run(spark, [_ev("PLT01-L1", "2025-11-16 23:00:00", "2025-11-17 02:00:00")])
    r = out[("PLT01-L1", "2025-11-16", "S2", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 180)
    assert len(out) == 1


def test_earlier_as_of_changes_transform_output(spark):
    events = [_ev("PLT01-L1", "2025-11-09 20:00:00", "2025-11-09 21:00:00"),
              _ev("PLT01-L1", "2025-11-16 22:00:00", None)]
    feeds = _feeds(spark, events)
    early = sorted(job.transform(feeds, "2025-11-10 00:00:00").collect())
    late = sorted(job.transform(feeds, AS_OF).collect())
    assert early != late


# ------------------------------------------------------------ joins / drops

def test_unknown_line_is_dropped_in_staging(spark):
    feeds = _feeds(spark, [_ev("PLT99-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00")])
    assert _stg(feeds).count() == 0


def test_unknown_reason_survives_split_but_dropped_in_report(spark):
    feeds = _feeds(spark, [_ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00",
                               "R-XX", 0)])
    assert _seg(feeds).count() == 1
    assert job.build_report(_seg(feeds), feeds["dim.downtime_reason"]).count() == 0


def test_rows_violating_not_null_contract_are_dropped(spark):
    # NULL end_utc is NOT a violation: it is an open event.
    out = _run(spark, [
        _ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00"),   # kept
        _ev("PLT01-L1", "2025-10-21 20:00:00", None),                    # kept (open)
        _ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00", "R-JAM", None),
        _ev(None, "2025-10-21 19:00:00", "2025-10-21 20:00:00"),
        _ev("PLT01-L1", None, "2025-10-21 20:00:00"),
        _ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00", None),
        (None, "PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00", "R-JAM", 0),
    ], as_of="2025-10-21 21:00:00")
    # kept rows both land in S2 14:00 -> 16:00 local: 60 + 60 minutes
    r = out[("PLT01-L1", "2025-10-21", "S2", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (2, 120)
    assert len(out) == 1


def test_char_padding_in_line_plant_id_still_joins(spark):
    # dim.line.plant_id comes through CHAR(5)-padded; TRIM + inner join.
    lines = [("PLT01-L1", "PLT01 ", "Line 1")]
    out = _run(spark, [_ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00")],
               lines=lines)
    r = out[("PLT01-L1", "2025-10-21", "S2", "Mechanical", False)]
    assert r.plant_id == "PLT01"


# --------------------------------------------------------------- report grain

def test_event_count_is_distinct_events(spark):
    out = _run(spark, [
        _ev("PLT01-L1", "2025-10-21 12:00:00", "2025-10-21 12:30:00"),  # 30
        _ev("PLT01-L1", "2025-10-21 13:00:00", "2025-10-21 13:15:00"),  # 15
    ])
    r = out[("PLT01-L1", "2025-10-21", "S1", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (2, 45)


def test_duplicate_event_id_counts_once_but_minutes_double(spark):
    # PK violation is impossible in prod; COUNT(DISTINCT) does not dedupe SUM.
    out = _run(spark, [
        (7, "PLT01-L1", "2025-10-21 12:00:00", "2025-10-21 12:30:00", "R-JAM", 0),
        (7, "PLT01-L1", "2025-10-21 12:00:00", "2025-10-21 12:30:00", "R-JAM", 0),
    ])
    r = out[("PLT01-L1", "2025-10-21", "S1", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 60)


def test_planned_flag_comes_from_event_not_reason_dim(spark):
    # R-CO has planned_default 1, but the report carries the EVENT's flag.
    out = _run(spark, [_ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00",
                           "R-CO", 0)])
    assert set(out) == {("PLT01-L1", "2025-10-21", "S2", "Changeover", False)}


def test_planned_flag_is_part_of_the_grain(spark):
    out = _run(spark, [
        _ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00", "R-CO", 0),
        _ev("PLT01-L1", "2025-10-21 20:00:00", "2025-10-21 21:00:00", "R-CO", 1),
    ])
    assert set(out) == {("PLT01-L1", "2025-10-21", "S2", "Changeover", False),
                        ("PLT01-L1", "2025-10-21", "S2", "Changeover", True)}


def test_zero_length_event_inside_shift_keeps_a_row(spark):
    out = _run(spark, [_ev("PLT01-L1", "2025-10-21 15:00:00", "2025-10-21 15:00:00")])
    r = out[("PLT01-L1", "2025-10-21", "S1", "Mechanical", False)]
    assert (r.event_count, r.downtime_minutes) == (1, 0)


def test_zero_length_event_on_shift_boundary_gets_no_segment(spark):
    # local 14:00:00 -> 14:00:00: strict < / > overlap drops it from both shifts
    out = _run(spark, [_ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 19:00:00")])
    assert out == {}


# -------------------------------------------------------------------- schema

def test_stage_downtime_local_schema(spark):
    feeds = _feeds(spark, [_ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00")])
    stg = _stg(feeds)
    assert tuple(stg.columns) == job.STG_DOWNTIME_LOCAL_COLUMNS
    assert stg.dtypes == [("event_id", "int"), ("plant_id", "string"),
                          ("line_id", "string"), ("reason_code", "string"),
                          ("planned_flag", "boolean"), ("start_utc", "timestamp"),
                          ("end_utc", "timestamp"), ("start_local", "timestamp"),
                          ("end_local", "timestamp")]


def test_split_downtime_by_shift_schema(spark):
    feeds = _feeds(spark, [_ev("PLT01-L1", "2025-10-21 19:00:00", "2025-10-21 20:00:00")])
    seg = _seg(feeds)
    assert tuple(seg.columns) == job.STG_DOWNTIME_SHIFT_SEG_COLUMNS
    assert seg.dtypes == [("event_id", "int"), ("plant_id", "string"),
                          ("line_id", "string"), ("reason_code", "string"),
                          ("planned_flag", "boolean"), ("production_day", "date"),
                          ("shift_code", "string"), ("seg_start_local", "timestamp"),
                          ("seg_end_local", "timestamp")]


def test_transform_output_schema(spark):
    df = job.transform(_feeds(spark, [_ev("PLT01-L1", "2025-10-21 19:00:00",
                                        "2025-10-21 20:00:00")]), AS_OF)
    assert tuple(df.columns) == job.OUTPUT_COLUMNS
    assert df.dtypes == [("plant_id", "string"), ("line_id", "string"),
                         ("production_day", "date"), ("shift_code", "string"),
                         ("reason_category", "string"), ("planned_flag", "boolean"),
                         ("event_count", "int"), ("downtime_minutes", "int")]


def test_stages_compose_directly_and_are_pure(spark):
    # the KAN-7 contract: split_downtime_by_shift consumes
    # stage_downtime_local + daily_production.build_shift_calendar as-is
    feeds = _feeds(spark, [_ev("PLT01-L1", "2025-10-23 18:30:00", "2025-10-23 19:45:00")])
    seg = _seg(feeds)
    assert seg.count() == 2
    stg_again = _stg(feeds)
    seg_again = job.split_downtime_by_shift(stg_again, feeds["dim.plant"],
                                            _shift_calendar(feeds))
    assert sorted(stg_again.collect()) == sorted(_stg(feeds).collect())
    assert sorted(seg_again.collect()) == sorted(seg.collect())


# -------------------------------------------------------------- parse_as_of

@pytest.mark.parametrize("value", [
    "2025-11-17 00:00:00", "2025-11-17T00:00:00Z", "2025-11-17T00:00:00.0000000Z",
    "2025-11-16T18:00:00-06:00",
    "2025-11-16 23:59:59.5",    # DATETIME2(0): fractional rounds half-up
    datetime(2025, 11, 17),   # a datetime passes through
])
def test_parse_as_of_utc_formats(value):
    assert job.parse_as_of_utc(value) == datetime(2025, 11, 17)


def test_parse_as_of_utc_rejects_garbage():
    with pytest.raises(ValueError):
        job.parse_as_of_utc("nope")


# ---------------------------------------------------------------------- CLI

def test_cli_accepts_ns_and_as_of_utc():
    a = job.parse_args(["--ns", "x", "--as-of-utc", "2025-11-17 00:00:00"])
    assert (a.ns, a.as_of_utc) == ("x", "2025-11-17 00:00:00")


def test_cli_requires_as_of_utc():
    with pytest.raises(SystemExit):
        job.parse_args(["--ns", "x"])


def test_source_has_no_wall_clock_reads():
    src = inspect.getsource(job)
    for banned in ("current_timestamp", "current_date", "now(", "utcnow",
                   "time.time(", "datetime.today"):
        assert banned not in src
    assert "spark.sql.session.timeZone" not in src
