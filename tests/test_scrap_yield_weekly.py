"""Unit tests for mfg_lake.jobs.scrap_yield_weekly (PL_Scrap_Yield)."""
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw
from mfg_lake.common.spark import get_spark
from mfg_lake.jobs import daily_production
from mfg_lake.jobs import scrap_yield_weekly as job

AS_OF = "2025-11-17 00:00:00"
CST = "Central Standard Time"


@pytest.fixture(scope="module")
def spark():
    session = get_spark("test_scrap_yield_weekly")
    yield session
    session.stop()


def _stage(spark, rows):
    return spark.createDataFrame(
        [(line, plant, "SKU01", date.fromisoformat(day), "S1", 15, good, good)
         for line, plant, day, good in rows],
        daily_production.STG_PRODUCTION_LOCAL_COLUMNS)


def _frames(spark, buckets=(), scraps=(), *, plants=(("PLT01", CST),),
            days=("2025-10-26", "2025-10-27", "2025-10-28"),
            lines=None, patterns=None):
    plant_ids = [plant_id for plant_id, _ in plants]
    if lines is None:
        lines = [(f"{plant_id}-L1", f"{plant_id} ") for plant_id in plant_ids]
    if patterns is None:
        patterns = {}
    shift_rows = []
    default_shifts = (
        ("S1", "06:00", "14:00", 0),
        ("S2", "14:00", "22:00", 0),
        ("S3", "22:00", "06:00", 1),
    )
    for plant_id in plant_ids:
        shift_rows.extend((plant_id, *shift)
                          for shift in patterns.get(plant_id, default_shifts))
    return {
        "mes.scrap_event": spark.createDataFrame(
            scraps, "scrap_id int, line_id string, scrap_ts_utc string, qty_units int"),
        "mes.production_order": spark.createDataFrame(
            [], "order_id string, line_id string, sched_start_utc string, "
                "sched_end_utc string"),
        "dim.line": spark.createDataFrame(lines, "line_id string, plant_id string"),
        "dim.plant": spark.createDataFrame(
            [(plant_id + " ", tz_name) for plant_id, tz_name in plants],
            "plant_id string, tz_name string"),
        "dim.shift_pattern": spark.createDataFrame(
            shift_rows, "plant_id string, shift_code string, local_start string, "
                        "local_end string, end_next_day int"),
        "dim.calendar": spark.createDataFrame(
            [(day,) for day in days], "calendar_date string"),
        "mes.production_count": spark.createDataFrame(
            buckets, "line_id string, bucket_start_utc string, bucket_end_utc string, "
                     "sku_id string, total_units int, good_units int"),
    }


def _line(spark):
    return spark.createDataFrame(
        [("PLT01-L1", "PLT01 ")], "line_id string, plant_id string")


@pytest.mark.parametrize("day,expected", [
    ("2024-12-30", (2025, 1)),
    ("2021-01-03", (2020, 53)),
    ("2027-01-01", (2026, 53)),
    ("2025-12-28", (2025, 52)),
    ("2026-01-01", (2026, 1)),
    ("2025-10-20", (2025, 43)),
    ("2025-11-16", (2025, 46)),
])
def test_iso_week_and_year_around_jan_1(spark, day, expected):
    frame = spark.createDataFrame([(day,)], "day string")
    year, week = job.iso_week_and_year(F.to_date("day"))
    row = frame.select(year.alias("iso_year"), week.alias("iso_week")).first()
    assert (row.iso_year, row.iso_week) == expected


def test_scrap_week_uses_utc_timestamp_not_local_production_day(spark):
    stg = _stage(spark, [
        ("PLT01-L1", "PLT01", "2025-10-26", 1000),
        ("PLT01-L1", "PLT01", "2025-10-27", 2000),
    ])
    events = spark.createDataFrame(
        [(1, "PLT01-L1", "2025-10-27 03:00:00", 10)],
        "scrap_id int, line_id string, scrap_ts_utc string, qty_units int")
    got = {(row.iso_year, row.iso_week):
           (row.good_units, row.scrap_units, row.scrap_pct)
           for row in job.rpt_scrap_yield_weekly(events, _line(spark), stg).collect()}
    assert got == {
        (2025, 43): (1000, 0, Decimal("0.00")),
        (2025, 44): (2000, 10, Decimal("0.50")),
    }


def test_scrap_week_without_production_is_dropped_and_logged(spark, caplog):
    caplog.set_level(logging.INFO, logger=job.log.name)
    events = spark.createDataFrame(
        [(19, "PLT01-L1", "2025-10-27 03:00:00", 10)],
        "scrap_id int, line_id string, scrap_ts_utc string, qty_units int")
    stg = _stage(spark, [("PLT01-L1", "PLT01", "2025-10-20", 100)])
    result = job.rpt_scrap_yield_weekly(events, _line(spark), stg)
    assert result.count() == 1
    messages = [record.getMessage() for record in caplog.records]
    assert any("dropped 1 scrap group(s) with no good row" in message
               and "PLT01-L1" in message and "2025" in message and "44" in message
               for message in messages)


def test_week_without_scrap_reports_zero(spark):
    stg = _stage(spark, [("PLT01-L1", "PLT01", "2025-10-27", 20)])
    events = spark.createDataFrame(
        [], "scrap_id int, line_id string, scrap_ts_utc string, qty_units int")
    row = job.rpt_scrap_yield_weekly(events, _line(spark), stg).first()
    assert (row.scrap_units, row.scrap_pct) == (0, Decimal("0.00"))


def test_scrap_on_unknown_line_is_dropped_and_logged(spark, caplog):
    caplog.set_level(logging.INFO, logger=job.log.name)
    events = spark.createDataFrame(
        [(91, "UNKNOWN", "2025-10-27 03:00:00", 10)],
        "scrap_id int, line_id string, scrap_ts_utc string, qty_units int")
    stg = _stage(spark, [("PLT01-L1", "PLT01", "2025-10-27", 100)])
    assert job.rpt_scrap_yield_weekly(events, _line(spark), stg).count() == 1
    assert any("dropped 1 scrap event(s) at dim.line inner join"
               in record.getMessage()
               and "91" in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize("scrap,good,expected", [
    (1, 799, Decimal("0.13")),
    (1, 19999, Decimal("0.01")),
    (1, 1999, Decimal("0.05")),
    (3, 397, Decimal("0.75")),
    (5, 0, Decimal("100.00")),
    (2, 2, Decimal("50.00")),
])
def test_scrap_pct_decimal_rounding_half_away_from_zero(
        spark, scrap, good, expected):
    events = spark.createDataFrame(
        [(1, "PLT01-L1", "2025-10-27 03:00:00", scrap)],
        "scrap_id int, line_id string, scrap_ts_utc string, qty_units int")
    stg = _stage(spark, [("PLT01-L1", "PLT01", "2025-10-27", good)])
    assert job.rpt_scrap_yield_weekly(events, _line(spark), stg).first().scrap_pct \
        == expected


def test_zero_denominator_scrap_pct_is_null(spark):
    events = spark.createDataFrame(
        [], "scrap_id int, line_id string, scrap_ts_utc string, qty_units int")
    stg = _stage(spark, [("PLT01-L1", "PLT01", "2025-10-27", 0)])
    row = job.rpt_scrap_yield_weekly(events, _line(spark), stg).first()
    assert (row.scrap_units, row.scrap_pct) == (0, None)


def test_int_overflow_raises(spark):
    events_empty = spark.createDataFrame(
        [], "scrap_id int, line_id string, scrap_ts_utc string, qty_units int")
    stg_overflow = _stage(spark, [
        ("PLT01-L1", "PLT01", "2025-10-27", 2_147_483_647),
        ("PLT01-L1", "PLT01", "2025-10-27", 1),
    ])
    with pytest.raises(OverflowError, match="Arithmetic overflow converting expression"):
        job.rpt_scrap_yield_weekly(events_empty, _line(spark), stg_overflow)

    stg_max = _stage(spark, [("PLT01-L1", "PLT01", "2025-10-27", 2_147_483_647)])
    event_one = spark.createDataFrame(
        [(1, "PLT01-L1", "2025-10-27 03:00:00", 1)],
        "scrap_id int, line_id string, scrap_ts_utc string, qty_units int")
    with pytest.raises(OverflowError, match="Arithmetic overflow converting expression"):
        job.rpt_scrap_yield_weekly(event_one, _line(spark), stg_max)


def test_build_report_end_to_end_schema_and_trim(spark):
    frames = _frames(
        spark,
        [
            ("PLT01-L1", "2025-10-26 15:00:00", "2025-10-26 15:15:00",
             "SKU01", 100, 100),
            ("PLT01-L1", "2025-10-27 15:00:00", "2025-10-27 15:15:00",
             "SKU01", 200, 200),
            ("PLT02-L1", "2025-10-28 15:00:00", "2025-10-28 15:15:00",
             "SKU01", 100, 100),
        ],
        [
            (1, "PLT01-L1", "2025-10-27 03:00:00", 10),
            (2, "PLT02-L1", "2025-10-28 15:00:00", 5),
        ],
        plants=(("PLT01", CST), ("PLT02", CST)))
    report = job.build_report(frames, AS_OF)
    assert report.dtypes == [
        ("plant_id", "string"), ("line_id", "string"), ("iso_year", "int"),
        ("iso_week", "int"), ("good_units", "int"), ("scrap_units", "int"),
        ("scrap_pct", "decimal(9,2)")]
    got = {(row.plant_id, row.line_id, row.iso_year, row.iso_week):
           (row.good_units, row.scrap_units, row.scrap_pct)
           for row in report.collect()}
    assert got == {
        ("PLT01", "PLT01-L1", 2025, 43): (100, 0, Decimal("0.00")),
        ("PLT01", "PLT01-L1", 2025, 44): (200, 10, Decimal("4.76")),
        ("PLT02", "PLT02-L1", 2025, 44): (100, 5, Decimal("4.76")),
    }


def test_as_of_is_validated_but_does_not_filter(spark):
    frames = _frames(
        spark,
        [("PLT01-L1", "2025-10-27 15:00:00", "2025-10-27 15:15:00",
          "SKU01", 100, 100)],
        [(1, "PLT01-L1", "2025-10-27 16:00:00", 10)])
    later = job.build_report(frames, AS_OF)
    earlier = job.build_report(frames, "2025-10-01 00:00:00")
    assert later.collect() == earlier.collect()
    with pytest.raises(ValueError, match="unparseable --as-of-utc"):
        job.build_report(frames, "garbage")


def test_full_seed_report_matches_snapshot_shape(spark):
    frames = {table: read_raw(spark, table) for table in job.SOURCES}
    report = job.build_report(frames, AS_OF).cache()
    assert report.count() == 96
    assert report.select("plant_id", "line_id").distinct().count() == 24
    assert {row.iso_week for row in report.select("iso_week").distinct().collect()} \
        == {43, 44, 45, 46}
    assert report.agg(F.sum("scrap_units")).first()[0] == \
        frames["mes.scrap_event"].agg(F.sum("qty_units")).first()[0]


def test_job_source_never_reads_wall_clock():
    source = (Path(__file__).resolve().parents[1] / "lakehouse" / "src" /
              "mfg_lake" / "jobs" / "scrap_yield_weekly.py").read_text()
    lowered = source.lower()
    forbidden = ("current_timestamp", "now(", "datetime.now", "time.time", "sysdate")
    assert not any(token in lowered for token in forbidden)


def _alloc_frames(spark, scraps, orders):
    return (
        spark.createDataFrame(
            scraps, "scrap_id int, line_id string, scrap_ts_utc string, qty_units int"),
        spark.createDataFrame(
            orders, "order_id string, line_id string, sched_start_utc string, "
                    "sched_end_utc string"))


def _alloc_rows(result):
    return sorted((row.scrap_id, row.order_id, row.qty_units)
                  for row in result.collect())


def test_alloc_single_order_full_qty(spark):
    scraps, orders = _alloc_frames(
        spark,
        [(1, "PLT01-L1", "2025-10-27 12:00:00", 100)],
        [("ORD-A", "PLT01-L1", "2025-10-27 08:00:00",
          "2025-10-27 20:00:00")])
    assert _alloc_rows(job.stg_scrap_alloc(scraps, orders)) == [(1, "ORD-A", 100)]


def test_alloc_window_clipped_to_7200_seconds(spark):
    scraps, orders = _alloc_frames(
        spark,
        [(1, "PLT01-L1", "2025-10-27 12:00:00", 19)],
        [
            ("ORD-A", "PLT01-L1", "2025-10-27 00:00:00",
             "2025-10-27 23:00:00"),
            ("ORD-B", "PLT01-L1", "2025-10-27 11:00:00",
             "2025-10-27 12:30:00"),
        ])
    # A full-duration interpretation would allocate A=18 and B=1.
    assert _alloc_rows(job.stg_scrap_alloc(scraps, orders)) == [
        (1, "ORD-A", 14), (1, "ORD-B", 5)]


def test_alloc_remainder_to_largest_overlap(spark):
    scraps, orders = _alloc_frames(
        spark,
        [(1, "PLT01-L1", "2025-10-27 12:00:00", 100)],
        [
            ("ORD-A", "PLT01-L1", "2025-10-27 08:00:00",
             "2025-10-27 13:00:00"),
            ("ORD-B", "PLT01-L1", "2025-10-27 11:30:00",
             "2025-10-27 20:00:00"),
        ])
    assert _alloc_rows(job.stg_scrap_alloc(scraps, orders)) == [
        (1, "ORD-A", 55), (1, "ORD-B", 45)]


def test_alloc_tie_broken_by_order_id(spark):
    scraps, orders = _alloc_frames(
        spark,
        [(1, "PLT01-L1", "2025-10-27 12:00:00", 101)],
        [
            ("ORD-300002", "PLT01-L1", "2025-10-27 08:00:00",
             "2025-10-27 13:00:00"),
            ("ORD-300001", "PLT01-L1", "2025-10-27 11:00:00",
             "2025-10-27 20:00:00"),
        ])
    assert _alloc_rows(job.stg_scrap_alloc(scraps, orders)) == [
        (1, "ORD-300001", 51), (1, "ORD-300002", 50)]


def test_alloc_remainder_not_spread(spark):
    scraps, orders = _alloc_frames(
        spark,
        [(1, "PLT01-L1", "2025-10-27 12:00:00", 101)],
        [
            ("ORD-1", "PLT01-L1", "2025-10-27 08:00:00",
             "2025-10-27 13:00:00"),
            ("ORD-2", "PLT01-L1", "2025-10-27 11:00:00",
             "2025-10-27 20:00:00"),
            ("ORD-3", "PLT01-L1", "2025-10-27 10:00:00",
             "2025-10-27 13:00:00"),
        ])
    assert _alloc_rows(job.stg_scrap_alloc(scraps, orders)) == [
        (1, "ORD-1", 35), (1, "ORD-2", 33), (1, "ORD-3", 33)]


def test_alloc_between_is_inclusive(spark):
    scraps, orders = _alloc_frames(
        spark,
        [
            (1, "PLT01-L1", "2025-10-27 12:00:00", 11),
            (2, "PLT01-L2", "2025-10-27 12:00:00", 13),
            (3, "PLT01-L3", "2025-10-27 12:00:00", 17),
        ],
        [
            ("ORD-END", "PLT01-L1", "2025-10-27 06:00:00",
             "2025-10-27 12:00:00"),
            ("ORD-START", "PLT01-L2", "2025-10-27 12:00:00",
             "2025-10-27 18:00:00"),
            ("ORD-PAST", "PLT01-L3", "2025-10-27 10:00:00",
             "2025-10-27 11:59:59"),
        ])
    assert _alloc_rows(job.stg_scrap_alloc(scraps, orders)) == [
        (1, "ORD-END", 11), (2, "ORD-START", 13), (3, "UNALLOCATED", 17)]


def test_alloc_other_line_orders_ignored_and_unallocated_logged(spark, caplog):
    caplog.set_level(logging.INFO, logger=job.log.name)
    scraps, orders = _alloc_frames(
        spark,
        [(81, "PLT01-L1", "2025-10-27 12:00:00", 9)],
        [("ORD-OTHER", "PLT01-L2", "2025-10-27 08:00:00",
          "2025-10-27 20:00:00")])
    assert _alloc_rows(job.stg_scrap_alloc(scraps, orders)) == [
        (81, "UNALLOCATED", 9)]
    assert any(
        "1 scrap event(s) with no overlapping order -> UNALLOCATED; "
        "sample scrap_ids=[81]" in record.getMessage()
        for record in caplog.records)


def test_alloc_zero_total_overlap_raises(spark):
    scraps, orders = _alloc_frames(
        spark,
        [(1, "PLT01-L1", "2025-10-27 12:00:00", 9)],
        [("ORD-ZERO", "PLT01-L1", "2025-10-27 12:00:00",
          "2025-10-27 12:00:00")])
    with pytest.raises(ZeroDivisionError, match="Divide by zero error encountered"):
        job.stg_scrap_alloc(scraps, orders)


def test_alloc_int_overflow_raises(spark):
    scraps, orders = _alloc_frames(
        spark,
        [(1, "PLT01-L1", "2025-10-27 12:00:00", 2_000_000)],
        [("ORD-WIDE", "PLT01-L1", "2025-10-27 10:00:00",
          "2025-10-27 14:00:00")])
    with pytest.raises(
            OverflowError,
            match="Arithmetic overflow error converting expression to data type int"):
        job.stg_scrap_alloc(scraps, orders)


def test_alloc_trims_ids_and_schema(spark):
    scraps, orders = _alloc_frames(
        spark,
        [(1, " PLT01-L1 ", "2025-10-27 12:00:00", 5)],
        [(" ORD-A ", "PLT01-L1 ", "2025-10-27 08:00:00",
          "2025-10-27 20:00:00")])
    result = job.stg_scrap_alloc(scraps, orders)
    assert result.dtypes == [
        ("scrap_id", "int"), ("order_id", "string"), ("qty_units", "int")]
    assert _alloc_rows(result) == [(1, "ORD-A", 5)]


def _alloc_cursor_port(scrap_rows, order_rows):
    """Literal Python port of the allocation proc's per-event cursor."""
    allocations = []
    two_hours = timedelta(seconds=7200)
    for scrap in sorted(scrap_rows, key=lambda row: row.scrap_id):
        ts = scrap.scrap_ts_utc
        candidates = []
        for order in order_rows:
            if order.line_id != scrap.line_id:
                continue
            start, end = order.sched_start_utc, order.sched_end_utc
            if not (start < ts + two_hours
                    and end > ts - two_hours
                    and start <= ts <= end):
                continue
            ov_start = max(start, ts - two_hours)
            ov_end = min(end, ts + two_hours)
            ov_secs = int((ov_end - ov_start).total_seconds())
            candidates.append((order.order_id, ov_secs))
        if not candidates:
            allocations.append((scrap.scrap_id, job.UNALLOCATED, scrap.qty_units))
            continue
        total = sum(ov_secs for _, ov_secs in candidates)
        inserted = [
            (scrap.scrap_id, order_id,
             int(Fraction(scrap.qty_units * ov_secs, total)))
            for order_id, ov_secs in candidates
        ]
        remainder = scrap.qty_units - sum(row[2] for row in inserted)
        largest_order_id = sorted(candidates, key=lambda row: (-row[1], row[0]))[0][0]
        allocations.extend(
            (scrap_id, order_id, qty_units
             + (remainder if order_id == largest_order_id else 0))
            for scrap_id, order_id, qty_units in inserted)
    return allocations


def test_alloc_matches_literal_cursor_port_on_seed(spark):
    scraps = read_raw(spark, "mes.scrap_event")
    orders = read_raw(spark, "mes.production_order")
    expected_rows = _alloc_cursor_port(scraps.collect(), orders.collect())
    expected = spark.createDataFrame(
        expected_rows, "scrap_id int, order_id string, qty_units int")
    actual = job.stg_scrap_alloc(scraps, orders)
    assert actual.exceptAll(expected).count() == 0
    assert expected.exceptAll(actual).count() == 0
    assert actual.count() == 738
    assert actual.where(F.col("order_id") == job.UNALLOCATED).count() == 446
    assert actual.where(F.col("order_id") != job.UNALLOCATED).count() == 292
    source_totals = (scraps.groupBy("scrap_id")
                     .agg(F.sum(F.col("qty_units").cast("bigint"))
                          .alias("qty_units")))
    allocated_totals = (actual.groupBy("scrap_id")
                        .agg(F.sum(F.col("qty_units").cast("bigint"))
                             .alias("qty_units")))
    assert source_totals.exceptAll(allocated_totals).count() == 0
    assert allocated_totals.exceptAll(source_totals).count() == 0
