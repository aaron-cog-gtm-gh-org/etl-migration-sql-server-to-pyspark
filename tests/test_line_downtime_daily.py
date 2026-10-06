"""Unit tests for mfg_lake.jobs.line_downtime_daily (PL_Line_Downtime)."""
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw
from mfg_lake.common.spark import get_spark
from mfg_lake.common.tz import WINDOWS_TO_IANA, windows_to_iana
from mfg_lake.jobs import line_downtime_daily as job

AS_OF = "2025-11-17 00:00:00"
CHI = "Central Standard Time"


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test_line_downtime_daily")
    yield s
    s.stop()


def _frames(spark, events, *, plants=(("PLT01", CHI),), reasons=None,
            days=("2025-10-23",)):
    """Minimal estate. events: (event_id, line_id, start_utc, end_utc|None, reason, planned)."""
    plant_ids = [p for p, _ in plants]
    return {
        "mes.downtime_event": spark.createDataFrame(
            events, "event_id int, line_id string, start_utc string, end_utc string, "
                    "reason_code string, planned_flag int"),
        "dim.line": spark.createDataFrame(
            [(f"{p}-L1", p) for p in plant_ids], "line_id string, plant_id string"),
        # CHAR(5) is space-padded on the legacy side
        "dim.plant": spark.createDataFrame(
            [(p + " ", tz) for p, tz in plants], "plant_id string, tz_name string"),
        "dim.shift_pattern": spark.createDataFrame(
            [(p, c, s, e, n) for p in plant_ids for c, s, e, n in
             (("S1", "06:00", "14:00", 0), ("S2", "14:00", "22:00", 0),
              ("S3", "22:00", "06:00", 1))],
            "plant_id string, shift_code string, local_start string, local_end string, "
            "end_next_day int"),
        "dim.calendar": spark.createDataFrame([(d,) for d in days], "calendar_date string"),
        "dim.downtime_reason": spark.createDataFrame(
            reasons or [("R-JAM", "Mechanical"), ("R-PM", "Planned")],
            "reason_code string, reason_category string"),
    }


def _rows(df):
    return {(r.shift_code, str(r.production_day), r.reason_category, r.planned_flag):
            (r.event_count, r.downtime_minutes) for r in df.collect()}


# ------------------------------------------------------------ DATEDIFF(MINUTE)
@pytest.mark.parametrize("start,end,expected", [
    ("2025-10-20 10:00:59", "2025-10-20 10:01:00", 1),   # 1s elapsed, 1 boundary
    ("2025-10-20 10:00:00", "2025-10-20 10:00:59", 0),   # 59s elapsed, 0 boundaries
    ("2025-10-20 10:00:30", "2025-10-20 10:02:29", 2),   # 119s elapsed -> 2, not 1
    ("2025-10-20 10:00:00", "2025-10-20 12:10:00", 130),
    ("2025-10-20 23:59:59", "2025-10-21 00:00:00", 1),   # crosses midnight
    ("2025-10-20 10:01:00", "2025-10-20 10:00:59", -1),  # negative like T-SQL
])
def test_datediff_minute_counts_boundaries(spark, start, end, expected):
    df = spark.createDataFrame([(start, end)], "s string, e string")
    got = df.select(job.datediff_minute(F.col("s").cast("timestamp"),
                                        F.col("e").cast("timestamp")).alias("m")).first().m
    assert got == expected


def test_datediff_minute_end_to_end(spark):
    # local 10:00:59 -> 10:01:00 CDT = 1 minute (seed edge case)
    out = job.build_report(_frames(spark, [
        (1, "PLT01-L1", "2025-10-23 15:00:59", "2025-10-23 15:01:00", "R-JAM", 0)]), AS_OF)
    assert _rows(out) == {("S1", "2025-10-23", "Mechanical", False): (1, 1)}


# ------------------------------------------------------------ tz conversion
def test_windows_to_iana():
    assert windows_to_iana("Central Standard Time") == "America/Chicago"
    assert windows_to_iana("Central Standard Time (Mexico)") == "America/Mexico_City"
    assert windows_to_iana("GMT Standard Time") == "Europe/London"
    with pytest.raises(ValueError):
        windows_to_iana("Mars Standard Time")
    for iana in WINDOWS_TO_IANA.values():
        ZoneInfo(iana)  # every target is a real IANA zone


def test_unmapped_plant_tz_fails(spark):
    f = _frames(spark, [], plants=(("PLT09", "Mars Standard Time"),))
    with pytest.raises(ValueError, match="Mars Standard Time"):
        job.build_report(f, AS_OF)


def _local(spark, events, as_of=AS_OF, plants=(("PLT01", CHI),)):
    f = _frames(spark, events, plants=plants)
    p = job.plants_with_iana(f["dim.plant"])
    return {r.event_id: r for r in
            job.stage_downtime_local(f["mes.downtime_event"], f["dim.line"], p,
                                     as_of).collect()}


def test_chicago_cdt_cst_and_fall_back_fold(spark):
    rows = _local(spark, [
        (1, "PLT01-L1", "2025-10-20 13:00:00", "2025-10-20 14:00:00", "R-JAM", 0),  # CDT -5
        (2, "PLT01-L1", "2025-11-10 13:00:00", "2025-11-10 14:00:00", "R-JAM", 0),  # CST -6
        # 06:30Z and 07:30Z are both 01:30 local on 2025-11-02 (fold)
        (3, "PLT01-L1", "2025-11-02 06:30:00", "2025-11-02 07:30:00", "R-JAM", 0),
    ])
    assert rows[1].start_local == datetime(2025, 10, 20, 8, 0)
    assert rows[2].start_local == datetime(2025, 11, 10, 7, 0)
    assert rows[3].start_local == datetime(2025, 11, 2, 1, 30)
    assert rows[3].end_local == datetime(2025, 11, 2, 1, 30)
    assert rows[1].plant_id == "PLT01"  # CHAR(5) trimmed


def test_mexico_city_has_no_dst(spark):
    rows = _local(spark, [(1, "PLT04-L1", "2025-10-20 14:00:00", "2025-10-20 15:00:00",
                           "R-JAM", 0)],
                  plants=(("PLT04", "Central Standard Time (Mexico)"),))
    assert rows[1].start_local == datetime(2025, 10, 20, 8, 0)  # UTC-6 all year


def test_truncates_to_whole_seconds(spark):
    rows = _local(spark, [(1, "PLT01-L1", "2025-10-20 13:00:00.900",
                           "2025-10-20 13:30:15.999", "R-JAM", 0)])
    assert rows[1].start_utc == datetime(2025, 10, 20, 13, 0, 0)
    assert rows[1].end_local == datetime(2025, 10, 20, 8, 30, 15)


def test_dst_fall_back_minutes_use_local_wall_clock(spark):
    # seed edge case: local 00:30 CDT -> 02:30 CST = 180 elapsed minutes but
    # legacy DATEDIFF on local wall-clock gives 120 (snapshot PLT01-L1 11-01 S3)
    out = job.build_report(_frames(spark, [
        (1, "PLT01-L1", "2025-11-02 05:30:00", "2025-11-02 08:30:00", "R-JAM", 0)],
        days=("2025-11-01", "2025-11-02")), AS_OF)
    assert _rows(out) == {("S3", "2025-11-01", "Mechanical", False): (1, 120)}


def test_shift_calendar_fall_back_night_is_nine_hours(spark):
    f = _frames(spark, [], days=("2025-11-01",))
    sc = {r.shift_code: r for r in job.build_shift_calendar(
        job.plants_with_iana(f["dim.plant"]), f["dim.shift_pattern"],
        f["dim.calendar"]).collect()}
    assert sc["S3"].start_utc == datetime(2025, 11, 2, 3, 0)    # 22:00 CDT
    assert sc["S3"].end_utc == datetime(2025, 11, 2, 12, 0)     # 06:00 CST
    assert sc["S1"].production_day == date(2025, 11, 1)


# ------------------------------------------------------------ AsOfUtc capping
def test_open_event_capped_at_as_of(spark):
    rows = _local(spark, [
        (1, "PLT01-L1", "2025-11-16 20:00:00", None, "R-JAM", 0),                    # open
        (2, "PLT01-L1", "2025-11-16 23:50:00", "2025-11-17 00:30:00", "R-JAM", 0),  # closed after cutoff
        (3, "PLT01-L1", "2025-11-17 00:00:00", None, "R-JAM", 0),                    # starts at cutoff
    ])
    assert set(rows) == {1, 2}
    assert rows[1].end_utc == datetime(2025, 11, 17, 0, 0) and rows[1].capped_at_as_of
    # ISNULL only caps open events; closed ones past the cutoff are kept as-is
    assert rows[2].end_utc == datetime(2025, 11, 17, 0, 30) and not rows[2].capped_at_as_of


def test_open_event_minutes_end_to_end(spark):
    # 2025-11-16 20:00Z = 14:00 CST; capped at 18:00 CST -> 240 min in S2
    out = job.build_report(_frames(spark, [
        (1, "PLT01-L1", "2025-11-16 20:00:00", None, "R-JAM", 0)],
        days=("2025-11-16",)), "2025-11-17T00:00:00.0000000Z")
    assert _rows(out) == {("S2", "2025-11-16", "Mechanical", False): (1, 240)}


@pytest.mark.parametrize("raw,expected", [
    ("2025-11-17 00:00:00", "2025-11-17 00:00:00"),
    ("2025-11-17T06:30:00Z", "2025-11-17 06:30:00"),
    ("2025-11-17T06:30:00.0000000Z", "2025-11-17 06:30:00"),
    ("2025-11-17T06:30:00.5Z", "2025-11-17 06:30:01"),
    ("2025-11-17T00:30:00-06:00", "2025-11-17 06:30:00"),
])
def test_parse_as_of_utc(raw, expected):
    assert job.parse_as_of_utc(raw) == expected


def test_as_of_injected_as_literal_not_current_timestamp():
    import inspect
    src = inspect.getsource(job)
    assert "current_timestamp" not in src
    assert "now()" not in src


# ------------------------------------------------------------ shift split
def test_split_across_two_shifts(spark):
    # local 13:30 -> 14:45 CDT
    out = job.build_report(_frames(spark, [
        (1, "PLT01-L1", "2025-10-23 18:30:00", "2025-10-23 19:45:00", "R-JAM", 0)]), AS_OF)
    assert _rows(out) == {("S1", "2025-10-23", "Mechanical", False): (1, 30),
                          ("S2", "2025-10-23", "Mechanical", False): (1, 45)}


def test_split_across_three_shifts_and_production_days(spark):
    # local 2025-10-23 13:00 -> 2025-10-24 07:00 CDT
    out = job.build_report(_frames(spark, [
        (1, "PLT01-L1", "2025-10-23 18:00:00", "2025-10-24 12:00:00", "R-PM", 1)],
        days=("2025-10-23", "2025-10-24")), AS_OF)
    assert _rows(out) == {("S1", "2025-10-23", "Planned", True): (1, 60),
                          ("S2", "2025-10-23", "Planned", True): (1, 480),
                          ("S3", "2025-10-23", "Planned", True): (1, 480),
                          ("S1", "2025-10-24", "Planned", True): (1, 60)}


def test_event_count_is_distinct_events(spark):
    out = job.build_report(_frames(spark, [
        (1, "PLT01-L1", "2025-10-23 12:00:00", "2025-10-23 12:10:00", "R-JAM", 0),
        (2, "PLT01-L1", "2025-10-23 13:00:00", "2025-10-23 13:05:00", "R-JAM", 0)]), AS_OF)
    assert _rows(out) == {("S1", "2025-10-23", "Mechanical", False): (2, 15)}


# ------------------------------------------------------------ reason join
def test_unmatched_reason_code_dropped_and_logged(spark, caplog):
    caplog.set_level(logging.INFO, logger=job.log.name)
    out = job.build_report(_frames(spark, [
        (1, "PLT01-L1", "2025-10-23 12:00:00", "2025-10-23 12:10:00", "R-JAM", 0),
        (2, "PLT01-L1", "2025-10-23 18:30:00", "2025-10-23 19:45:00", "R-XXX", 0)]), AS_OF)
    assert _rows(out) == {("S1", "2025-10-23", "Mechanical", False): (1, 10)}
    msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("dropped 2 segment(s)" in m and "R-XXX" in m for m in msgs)


# ------------------------------------------------------------ output contract
def test_output_schema_matches_legacy_types(spark):
    out = job.build_report(_frames(spark, [
        (1, "PLT01-L1", "2025-10-23 12:00:00", "2025-10-23 12:10:00", "R-JAM", 0)]), AS_OF)
    assert out.dtypes == [
        ("plant_id", "string"), ("line_id", "string"), ("production_day", "date"),
        ("shift_code", "string"), ("reason_category", "string"),
        ("planned_flag", "boolean"), ("event_count", "int"), ("downtime_minutes", "int")]
    assert out.first().plant_id == "PLT01"


# ------------------------------------------------------------ UTC vs local join
def test_utc_join_equivalent_to_legacy_local_join_on_seed(spark):
    """Reconciles the UTC overlap join against a literal port of the legacy
    local-time join on the full seed feed (stage 2, segment level)."""
    frames = {t: read_raw(spark, t) for t in job.SOURCES}
    plants = job.plants_with_iana(frames["dim.plant"])
    local = job.stage_downtime_local(frames["mes.downtime_event"], frames["dim.line"],
                                     plants, AS_OF).drop("capped_at_as_of")
    sc = job.build_shift_calendar(plants, frames["dim.shift_pattern"], frames["dim.calendar"])
    cols = ["event_id", "plant_id", "line_id", "reason_code", "planned_flag",
            "production_day", "shift_code", "seg_start_local", "seg_end_local"]
    utc = job.split_by_shift(local, sc).select(cols)
    legacy = job.split_by_shift_local_legacy(local, sc).select(cols)
    assert utc.count() == legacy.count() > 0
    assert utc.exceptAll(legacy).count() == 0
    assert legacy.exceptAll(utc).count() == 0
