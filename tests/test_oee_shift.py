"""Unit tests for mfg_lake.jobs.oee_shift (PL_OEE)."""
import logging
import random
from datetime import date
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw
from mfg_lake.common.spark import get_spark
from mfg_lake.jobs import line_downtime_daily
from mfg_lake.jobs import oee_shift as job

AS_OF = "2025-11-17 00:00:00"
CHI = "Central Standard Time"
LONDON = "GMT Standard Time"


@pytest.fixture(scope="module")
def spark():
    session = get_spark("test_oee_shift")
    yield session
    session.stop()


def _round_fraction(value: Fraction | None, scale: int = 4) -> Decimal | None:
    if value is None:
        return None
    scaled = value * 10**scale
    sign = -1 if scaled.numerator < 0 else 1
    numerator, denominator = abs(scaled.numerator), scaled.denominator
    rounded = sign * ((2 * numerator + denominator) // (2 * denominator))
    return Decimal(rounded).scaleb(-scale).quantize(Decimal(1).scaleb(-scale))


def exact_oee_row(shift_minutes, unplanned_dt_min, total_units, good_units, ideal_units):
    ideal = Fraction(Decimal(ideal_units))
    availability = (None if shift_minutes == 0 else
                    Fraction(shift_minutes - unplanned_dt_min, shift_minutes))
    performance = None if ideal == 0 else Fraction(total_units, 1) / ideal
    quality = None if total_units == 0 else Fraction(good_units, total_units)
    oee = (None if shift_minutes == 0 or total_units == 0 or ideal == 0 else
           Fraction((shift_minutes - unplanned_dt_min) * good_units, shift_minutes) / ideal)
    return tuple(_round_fraction(value) for value in (
        availability, performance, quality, oee))


def _frames(spark, *, plants=(("PLT01", CHI),), days=("2025-10-23",),
            patterns=None, lines=None, events=(), production=(), skus=None):
    plant_ids = [plant_id for plant_id, _ in plants]
    if lines is None:
        lines = [(f"{plant_id}-L1", f"{plant_id} ") for plant_id in plant_ids]
    if patterns is None:
        patterns = {}
    shift_rows = []
    for plant_id in plant_ids:
        shifts = patterns.get(plant_id, (
            ("S1", "06:00", "14:00", 0),
            ("S2", "14:00", "22:00", 0),
            ("S3", "22:00", "06:00", 1),
        ))
        shift_rows.extend((plant_id, *shift) for shift in shifts)
    if skus is None:
        skus = [("SKU01", Decimal("240.00"))]
    plant_rows = [(plant_id + " ", tz_name) for plant_id, tz_name in plants]
    line_rows = [(line_id, plant_id + " ") for line_id, plant_id in lines]
    return {
        "mes.downtime_event": spark.createDataFrame(
            events, "event_id int, line_id string, start_utc string, end_utc string, "
                    "reason_code string, planned_flag int"),
        "mes.production_count": spark.createDataFrame(
            production, "line_id string, bucket_start_utc string, bucket_end_utc string, "
                       "sku_id string, total_units int, good_units int"),
        "dim.line": spark.createDataFrame(line_rows, "line_id string, plant_id string"),
        "dim.plant": spark.createDataFrame(
            plant_rows, "plant_id string, tz_name string"),
        "dim.shift_pattern": spark.createDataFrame(
            shift_rows, "plant_id string, shift_code string, local_start string, "
                        "local_end string, end_next_day int"),
        "dim.calendar": spark.createDataFrame(
            [(day,) for day in days], "calendar_date string"),
        "dim.sku": spark.createDataFrame(
            skus, "sku_id string, ideal_units_per_min decimal(9,2)"),
    }


def _planned_frame(spark, rows):
    return spark.createDataFrame(
        rows, "plant_id string, line_id string, production_day date, shift_code string, "
              "shift_minutes int, planned_dt_min int, unplanned_dt_min int")


def _production_local_frame(spark, rows):
    return spark.createDataFrame(
        rows, "plant_id string, line_id string, production_day date, shift_code string, "
              "sku_id string, bucket_minutes int, total_units int, good_units int")


def _sku_frame(spark, rows):
    return spark.createDataFrame(
        rows, "sku_id string, ideal_units_per_min decimal(9,2)")


def _direct_report(spark, planned_rows, production_rows, skus):
    return job.rpt_oee_shift(
        _planned_frame(spark, planned_rows),
        _production_local_frame(spark, production_rows),
        _sku_frame(spark, skus))


def _calendar(frames):
    plants = job.plants_with_iana(frames["dim.plant"])
    return plants, job.build_shift_calendar(
        plants, frames["dim.shift_pattern"], frames["dim.calendar"])


def _planned_from_events(frames, as_of=AS_OF):
    plants, calendar = _calendar(frames)
    local = line_downtime_daily.stage_downtime_local(
        frames["mes.downtime_event"], frames["dim.line"], plants, as_of)
    seg = line_downtime_daily.split_by_shift(local.drop("capped_at_as_of"), calendar)
    return job.stg_planned_time(calendar, frames["dim.line"], seg)


def _pt_key(row):
    return row.plant_id, row.line_id, row.production_day, row.shift_code


def _pt_row(day, shift, *, line="PLT01-L1", shift_minutes=480, planned=0, unplanned=0):
    return ("PLT01", line, date.fromisoformat(day), shift, shift_minutes, planned, unplanned)


def _prod_row(day, shift, sku="SKU01", *, line="PLT01-L1", minutes=15,
              total=3000, good=2900):
    return ("PLT01", line, date.fromisoformat(day), shift, sku, minutes, total, good)


def test_shift_minutes_use_utc_bounds(spark):
    frames = _frames(
        spark,
        plants=(("PLT01", CHI), ("PLT02", LONDON)),
        days=("2025-10-23", "2025-11-01", "2025-10-25", "2026-03-07"),
        patterns={"PLT02": (("N", "18:00", "06:00", 1),)},
    )
    got = {_pt_key(row): row.shift_minutes for row in _planned_from_events(frames).collect()}
    assert got[("PLT01", "PLT01-L1", date(2025, 10, 23), "S1")] == 480
    assert got[("PLT01", "PLT01-L1", date(2025, 11, 1), "S3")] == 540
    assert got[("PLT02", "PLT02-L1", date(2025, 10, 25), "N")] == 780
    assert got[("PLT01", "PLT01-L1", date(2026, 3, 7), "S3")] == 420


def test_fall_back_downtime_uses_local_wall_clock(spark):
    frames = _frames(
        spark, days=("2025-11-01",),
        events=[(1, "PLT01-L1", "2025-11-02 05:30:00", "2025-11-02 08:30:00",
                 "R", 0)])
    planned = _planned_from_events(frames).where(
        (F.col("production_day") == date(2025, 11, 1)) & (F.col("shift_code") == "S3"))
    pt = planned.first()
    assert (pt.shift_minutes, pt.unplanned_dt_min) == (540, 120)
    row = _direct_report(
        spark, [("PLT01", "PLT01-L1", date(2025, 11, 1), "S3", 540, 0, 120)],
        [_prod_row("2025-11-01", "S3")], [("SKU01", Decimal("240.00"))]).first()
    assert row.availability == Decimal("0.7778")


def test_segment_inside_repeated_hour_is_negative(spark):
    frames = _frames(
        spark, days=("2025-11-01",),
        events=[(1, "PLT01-L1", "2025-11-02 06:30:00", "2025-11-02 07:10:00",
                 "R", 0)])
    pt = _planned_from_events(frames).where(
        (F.col("production_day") == date(2025, 11, 1)) & (F.col("shift_code") == "S3")).first()
    assert (pt.shift_minutes, pt.unplanned_dt_min) == (540, -20)
    row = _direct_report(
        spark, [("PLT01", "PLT01-L1", date(2025, 11, 1), "S3", 540, 0, -20)],
        [_prod_row("2025-11-01", "S3")], [("SKU01", Decimal("240.00"))]).first()
    assert row.availability == Decimal("1.0370")


def test_downtime_minutes_count_minute_boundaries(spark):
    frames = _frames(
        spark, lines=(("PLT01-L1", "PLT01"), ("PLT01-L2", "PLT01")),
        events=[
            (1, "PLT01-L1", "2025-10-23 15:00:59", "2025-10-23 15:01:00", "R", 0),
            (2, "PLT01-L2", "2025-10-23 15:10:00", "2025-10-23 15:10:59", "R", 0),
        ])
    pt = _planned_from_events(frames).where(
        (F.col("production_day") == date(2025, 10, 23)) & (F.col("shift_code") == "S1"))
    got = {row.line_id: row.unplanned_dt_min for row in pt.collect()}
    assert got == {"PLT01-L1": 1, "PLT01-L2": 0}


def test_planned_and_unplanned_split_and_isnull(spark):
    frames = _frames(
        spark, lines=(("PLT01-L1", "PLT01"), ("PLT01-L2", "PLT01")),
        events=[
            (1, "PLT01-L1", "2025-10-23 15:00:00", "2025-10-23 15:30:00", "R", 1),
            (2, "PLT01-L1", "2025-10-23 16:00:00", "2025-10-23 16:45:00", "R", 0),
        ])
    staged = _planned_from_events(frames).where(
        (F.col("production_day") == date(2025, 10, 23)) & (F.col("shift_code") == "S1"))
    got = {row.line_id: (row.planned_dt_min, row.unplanned_dt_min) for row in staged.collect()}
    assert got == {"PLT01-L1": (30, 45), "PLT01-L2": (0, 0)}
    report = _direct_report(
        spark,
        [("PLT01", "PLT01-L1", date(2025, 10, 23), "S1", 480, 30, 45),
         ("PLT01", "PLT01-L2", date(2025, 10, 23), "S1", 480, 0, 0)],
        [_prod_row("2025-10-23", "S1", line="PLT01-L1"),
         _prod_row("2025-10-23", "S1", line="PLT01-L2")],
        [("SKU01", Decimal("240.00"))])
    result = {row.line_id: row.availability for row in report.collect()}
    assert result == {"PLT01-L1": Decimal("0.9063"), "PLT01-L2": Decimal("1.0000")}


def test_calendar_line_join_drops_plant_without_lines_logged(spark, caplog):
    frames = _frames(
        spark, plants=(("PLT01", CHI), ("PLT02", CHI)),
        lines=(("PLT01-L1", "PLT01"),), days=("2025-10-23",))
    caplog.set_level(logging.INFO, logger=job.log.name)
    plants, calendar = _calendar(frames)
    planned = job.stg_planned_time(calendar, frames["dim.line"], _empty_seg(spark))
    assert planned.count() == 3
    assert {row.plant_id for row in planned.collect()} == {"PLT01"}
    messages = [record.getMessage() for record in caplog.records]
    assert any("dropped 3 shift_calendar row(s) with no dim.line plant match" in msg
               for msg in messages)

    orphan = spark.createDataFrame(
        [(9, "PLT01", "PLT01-ORPHAN", date(2025, 10, 23), "S1", False,
          "2025-10-23 06:00:00", "2025-10-23 06:05:00")],
        "event_id int, plant_id string, line_id string, production_day date, "
        "shift_code string, planned_flag boolean, seg_start_local string, seg_end_local string")
    job.stg_planned_time(calendar, frames["dim.line"], orphan).count()
    assert any("dropped 1 downtime segment(s) with no calendar×line match" in
               record.getMessage() for record in caplog.records)


def _empty_seg(spark):
    return spark.createDataFrame(
        [], "event_id int, plant_id string, line_id string, production_day date, "
           "shift_code string, planned_flag boolean, seg_start_local string, "
           "seg_end_local string")


def test_shift_without_production_dropped_and_logged(spark, caplog):
    pt_rows = [_pt_row("2025-10-23", "S1"), _pt_row("2025-10-23", "S2")]
    prod_rows = [_prod_row("2025-10-23", "S1"),
                 _prod_row("2025-10-23", "S3")]
    caplog.set_level(logging.INFO, logger=job.log.name)
    out = _direct_report(spark, pt_rows, prod_rows, [("SKU01", Decimal("240.00"))])
    assert out.count() == 1
    messages = [record.getMessage() for record in caplog.records]
    assert any("dropped 1 planned_time row(s) with no production row" in msg
               for msg in messages)
    assert any("dropped 1 production row(s) with no planned_time row" in msg
               for msg in messages)


def test_hard_coded_min_production_day(spark):
    planned = [_pt_row("2025-10-19", "S1"), _pt_row("2025-10-20", "S1")]
    production = [_prod_row("2025-10-19", "S1"), _prod_row("2025-10-20", "S1")]
    rows = _direct_report(
        spark, planned, production, [("SKU01", Decimal("240.00"))]).collect()
    assert [row.production_day for row in rows] == [date(2025, 10, 20)]


def test_unknown_sku_dropped_from_prod_and_logged(spark, caplog):
    caplog.set_level(logging.INFO, logger=job.log.name)
    report = _direct_report(
        spark, [_pt_row("2025-10-23", "S1")],
        [_prod_row("2025-10-23", "S1", total=3000, good=2900),
         _prod_row("2025-10-23", "S1", sku="SKU-X", total=1000, good=900)],
        [("SKU01", Decimal("240.00"))])
    row = report.first()
    assert row.quality == Decimal("0.9667")
    assert row.performance == Decimal("0.8333")
    assert any("dropped 1 production_local row(s) at dim.sku inner join" in
               record.getMessage() and "SKU-X" in record.getMessage()
               for record in caplog.records)


def test_performance_is_runtime_weighted_decimal(spark):
    planned = [("PLT01", "PLT01-L1", date(2025, 10, 23), "S1", 480, 0, 45)]
    production = [
        _prod_row("2025-10-23", "S1", sku="A", total=3000, good=2925),
        _prod_row("2025-10-23", "S1", sku="A", total=3000, good=2925),
        _prod_row("2025-10-23", "S1", sku="B", total=2000, good=1950),
    ]
    report = _direct_report(
        spark, planned, production,
        [("A", Decimal("240.00")), ("B", Decimal("150.00"))])
    row = report.first()
    assert (row.availability, row.performance, row.quality, row.oee) == \
        exact_oee_row(480, 45, 8000, 7800, Decimal("9450.00")) == (
            Decimal("0.9063"), Decimal("0.8466"), Decimal("0.9750"), Decimal("0.7480"))

    weighted = _direct_report(
        spark, [_pt_row("2025-10-23", "S1")],
        [_prod_row("2025-10-23", "S1", sku="C", minutes=75, total=14000, good=14000)],
        [("C", Decimal("210.50"))]).first()
    assert weighted.performance == Decimal("0.8868")


def test_oee_rounds_exact_ratio(spark):
    row = _direct_report(
        spark,
        [("PLT01", "PLT01-L1", date(2025, 10, 23), "S1", 720, 0, 58)],
        [_prod_row("2025-10-23", "S1", minutes=1, total=39313, good=1833)],
        [("SKU01", Decimal("38522.33"))]).first()
    assert (row.availability, row.performance, row.quality, row.oee) == \
        exact_oee_row(720, 58, 39313, 1833, Decimal("38522.33")) == (
            Decimal("0.9194"), Decimal("1.0205"), Decimal("0.0466"), Decimal("0.0437"))


@pytest.mark.parametrize(
    "plant,line,day,shift,sm,udt,total,good,ideal,expected_oee", [
        ("PLT03", "PLT03-L1", "2025-10-23", "S3",
         480, 0, 80152, 78929, "91200.00", "0.8654"),
        ("PLT03", "PLT03-L3", "2025-11-01", "S2",
         480, 0, 101768, 99839, "113100.00", "0.8827"),
        ("PLT05", "PLT05-L2", "2025-10-24", "D",
         720, 0, 160417, 157863, "179400.00", "0.8799"),
        ("PLT05", "PLT05-L2", "2025-10-30", "N",
         720, 0, 162649, 159920, "183300.00", "0.8724"),
    ])
def test_seed_snapshot_oee_rounding_edge(
        spark, plant, line, day, shift, sm, udt, total, good, ideal, expected_oee):
    production_day = date.fromisoformat(day)
    row = _direct_report(
        spark, [(plant, line, production_day, shift, sm, 0, udt)],
        [(plant, line, production_day, shift, "SKU01", 1, total, good)],
        [("SKU01", Decimal(ideal))]).first()
    expected = exact_oee_row(sm, udt, total, good, Decimal(ideal))
    assert (row.availability, row.performance, row.quality, row.oee) == expected
    assert row.oee == Decimal(expected_oee)


def test_ratios_match_exact_fraction_oracle(spark):
    random.seed(7)
    rows = []
    production = []
    skus = []
    expected = {}
    for i in range(20_000):
        shift_minutes = random.choice([480, 540, 720, 780, 0])
        unplanned = random.randint(-30, 120)
        total = random.randint(0, 40000)
        good = random.randint(0, total)
        ideal = Decimal(random.randint(0, 9999999)) / Decimal(100)
        key = f"L{i}"
        sku = f"K{i}"
        oracle = exact_oee_row(shift_minutes, unplanned, total, good, ideal)
        if any(value is not None and abs(value) > Decimal("99999.9999")
               for value in oracle):
            continue
        rows.append(("PLT01", key, date(2025, 10, 23), "S1",
                     shift_minutes, 0, unplanned))
        production.append(("PLT01", key, date(2025, 10, 23), "S1",
                           sku, 1, total, good))
        skus.append((sku, ideal))
        expected[key] = oracle
    report = _direct_report(spark, rows, production, skus)
    actual = {row.line_id: (row.availability, row.performance, row.quality, row.oee)
              for row in report.collect()}
    assert actual == expected


def test_round_half_away_from_zero(spark):
    rows = _direct_report(
        spark,
        [_pt_row("2025-10-23", "S1"), _pt_row("2025-10-23", "S1", line="PLT01-L2")],
        [_prod_row("2025-10-23", "S1", total=32, good=1),
         _prod_row("2025-10-23", "S1", line="PLT01-L2", total=32, good=3)],
        [("SKU01", Decimal("240.00"))]).collect()
    assert {row.line_id: row.quality for row in rows} == {
        "PLT01-L1": Decimal("0.0313"), "PLT01-L2": Decimal("0.0938")}


def test_double_rounding_regression(spark):
    ideal = Decimal("1001636.29")
    exact = Fraction(123652, 1) / Fraction(ideal)
    assert _round_fraction(exact) == Decimal("0.1234")
    assert _round_fraction(Fraction(_round_fraction(exact, 6))) == Decimal("0.1235")
    row = _direct_report(
        spark, [_pt_row("2025-10-23", "S1")],
        [_prod_row("2025-10-23", "S1", minutes=1, total=123652, good=123652)],
        [("SKU01", ideal)]).first()
    assert row.oee == Decimal("0.1234")


def test_negative_availability_rounds_away_from_zero(spark):
    row = _direct_report(
        spark, [("PLT01", "PLT01-L1", date(2025, 10, 23), "S1", 32, 0, 33)],
        [_prod_row("2025-10-23", "S1", minutes=1, total=1, good=1)],
        [("SKU01", Decimal("1.00"))]).first()
    assert row.availability == Decimal("-0.0313")
    assert row.oee == Decimal("-0.0313")


def test_zero_denominators_are_null(spark):
    rows = _direct_report(
        spark,
        [("PLT01", "L1", date(2025, 10, 23), "S1", 480, 0, 0),
         ("PLT01", "L2", date(2025, 10, 23), "S1", 480, 0, 0),
         ("PLT01", "L3", date(2025, 10, 23), "S1", 0, 0, 0)],
        [("PLT01", "L1", date(2025, 10, 23), "S1", "A", 15, 0, 0),
         ("PLT01", "L2", date(2025, 10, 23), "S1", "B", 15, 10, 8),
         ("PLT01", "L3", date(2025, 10, 23), "S1", "A", 15, 10, 8)],
        [("A", Decimal("240.00")), ("B", Decimal("0.00"))]).collect()
    got = {row.line_id: (row.availability, row.performance, row.quality, row.oee)
           for row in rows}
    assert got["L1"] == (Decimal("1.0000"), Decimal("0.0000"), None, None)
    assert got["L2"] == (Decimal("1.0000"), None, Decimal("0.8000"), None)
    assert got["L3"] == (None, Decimal("0.0028"), Decimal("0.8000"), None)


def test_decimal_9_4_overflow_raises(spark):
    with pytest.raises(OverflowError, match="Arithmetic overflow error converting numeric"):
        _direct_report(
            spark, [_pt_row("2025-10-23", "S1")],
            [_prod_row("2025-10-23", "S1", minutes=1, total=2_000_000, good=1)],
            [("SKU01", Decimal("0.01"))]).collect()


def test_int_sum_overflow_raises(spark):
    with pytest.raises(OverflowError, match="Arithmetic overflow error converting expression"):
        _direct_report(
            spark, [_pt_row("2025-10-23", "S1")],
            [_prod_row("2025-10-23", "S1", total=2_000_000_000, good=1),
             _prod_row("2025-10-23", "S1", total=2_000_000_000, good=1)],
            [("SKU01", Decimal("240.00"))]).collect()


def test_as_of_cutoff_flows_through_downtime_stage_only(spark):
    frames = _frames(
        spark, days=("2025-10-23",),
        events=[
            (1, "PLT01-L1", "2025-10-23 15:00:00", None, "R", 0),
            (2, "PLT01-L1", "2025-10-23 16:30:00", "2025-10-23 17:00:00", "R", 0),
        ],
        production=[("PLT01-L1", "2025-10-23 17:00:00", "2025-10-23 17:15:00",
                     "SKU01", 100, 90)])
    report = job.build_report(frames, "2025-10-23 16:00:00")
    row = report.first()
    assert row.unplanned_dt_min == 60
    assert row.planned_min == 480
    assert row.plant_id == "PLT01"


def test_padded_plant_ids_trimmed(spark):
    frames = _frames(
        spark, production=[("PLT01-L1", "2025-10-23 13:00:00",
                            "2025-10-23 13:15:00", "SKU01", 32, 30)])
    assert job.build_report(frames, AS_OF).first().plant_id == "PLT01"


def test_report_schema_matches_legacy_types(spark):
    frames = _frames(
        spark, production=[("PLT01-L1", "2025-10-23 13:00:00",
                            "2025-10-23 13:15:00", "SKU01", 32, 30)])
    report = job.build_report(frames, AS_OF)
    assert report.columns == job.OUTPUT_COLUMNS
    assert report.dtypes == [
        ("plant_id", "string"), ("line_id", "string"), ("production_day", "date"),
        ("shift_code", "string"), ("planned_min", "int"), ("unplanned_dt_min", "int"),
        ("availability", "decimal(9,4)"), ("performance", "decimal(9,4)"),
        ("quality", "decimal(9,4)"), ("oee", "decimal(9,4)")]
    plants, calendar = _calendar(frames)
    planned = job.stg_planned_time(calendar, frames["dim.line"], _empty_seg(spark))
    assert planned.columns == job.PLANNED_TIME_COLUMNS
    assert planned.dtypes == [
        ("plant_id", "string"), ("line_id", "string"), ("production_day", "date"),
        ("shift_code", "string"), ("shift_minutes", "int"),
        ("planned_dt_min", "int"), ("unplanned_dt_min", "int")]


def test_job_has_no_wall_clock_calls():
    path = Path(__file__).resolve().parents[1] / "lakehouse" / "src" / "mfg_lake" / \
        "jobs" / "oee_shift.py"
    text = path.read_text()
    forbidden = ("current_timestamp", "now(", "current_date", "time.time(")
    assert not any(token in text for token in forbidden), path


def test_full_seed_planned_time_utc_vs_legacy_local_split(spark):
    frames = {table: read_raw(spark, table) for table in job.SOURCES}
    plants, calendar = _calendar(frames)
    local = line_downtime_daily.stage_downtime_local(
        frames["mes.downtime_event"], frames["dim.line"], plants,
        "2025-11-17 00:00:00").cache()
    seg_utc = line_downtime_daily.split_by_shift(local.drop("capped_at_as_of"), calendar)
    seg_legacy = line_downtime_daily.split_by_shift_local_legacy(
        local.drop("capped_at_as_of"), calendar)
    pt_utc = job.stg_planned_time(calendar, frames["dim.line"], seg_utc)
    pt_legacy = job.stg_planned_time(calendar, frames["dim.line"], seg_legacy)
    assert pt_utc.exceptAll(pt_legacy).count() == 0
    assert pt_legacy.exceptAll(pt_utc).count() == 0
    assert job.build_report(frames, "2025-11-17 00:00:00").count() == 1792
