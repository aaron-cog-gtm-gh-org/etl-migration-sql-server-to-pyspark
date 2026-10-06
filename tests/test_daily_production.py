"""Unit tests for mfg_lake.jobs.daily_production (PL_Daily_Production)."""
import logging
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from mfg_lake.common.io import read_raw
from mfg_lake.common.spark import get_spark
from mfg_lake.jobs import daily_production as job

AS_OF = "2025-11-17 00:00:00"
CHI = "Central Standard Time"


@pytest.fixture(scope="module")
def spark():
    session = get_spark("test_daily_production")
    yield session
    session.stop()


def _frames(spark, buckets=(), *, plants=(("PLT01", CHI),),
            days=("2025-10-23",), patterns=None, lines=None, skus=None):
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
        skus = sorted({row[3] for row in buckets}) or [("SKU01", 12)]
        if skus and isinstance(skus[0], str):
            skus = [(sku_id, 12) for sku_id in skus]
    return {
        "mes.production_count": spark.createDataFrame(
            buckets, "line_id string, bucket_start_utc string, bucket_end_utc string, "
                     "sku_id string, total_units int, good_units int"),
        "dim.line": spark.createDataFrame(
            lines, "line_id string, plant_id string"),
        "dim.plant": spark.createDataFrame(
            [(plant_id + " ", tz_name) for plant_id, tz_name in plants],
            "plant_id string, tz_name string"),
        "dim.shift_pattern": spark.createDataFrame(
            shift_rows, "plant_id string, shift_code string, local_start string, "
                        "local_end string, end_next_day int"),
        "dim.calendar": spark.createDataFrame(
            [(day,) for day in days], "calendar_date string"),
        "dim.sku": spark.createDataFrame(
            skus, "sku_id string, pack_size int"),
    }


def _bucket(start, end=None, *, line="PLT01-L1", sku="SKU01", total=32, good=1):
    if end is None:
        end = start
    return (line, start, end, sku, total, good)


def _stage(frames):
    plants = job.plants_with_iana(frames["dim.plant"])
    calendar = job.build_shift_calendar(
        plants, frames["dim.shift_pattern"], frames["dim.calendar"])
    return job.stg_production_local(
        frames["mes.production_count"], frames["dim.line"], plants, calendar)


def _result_map(df):
    return {(row.plant_id, row.line_id, str(row.production_day), row.sku_id):
            (row.total_units, row.good_units, row.cases, row.yield_pct)
            for row in df.collect()}


@pytest.mark.parametrize("local,expected", [
    ("2025-10-23 06:00:00", date(2025, 10, 23)),
    ("2025-10-23 05:59:59", date(2025, 10, 22)),
    ("2025-10-23 23:00:00", date(2025, 10, 23)),
    ("2025-10-24 02:00:00", date(2025, 10, 23)),
])
def test_ufn_production_day(spark, local, expected):
    frame = spark.createDataFrame([(local,)], "local string")
    got = frame.select(job.ufn_production_day(
        F.col("local").cast("timestamp")).alias("day")).first().day
    assert got == expected


def test_bucket_straddling_production_day_boundary_uses_start(spark):
    frames = _frames(
        spark, [_bucket("2025-10-23 10:45:00", "2025-10-23 11:15:00")],
        days=("2025-10-22",))
    row = _stage(frames).first()
    assert (row.production_day, row.shift_code) == (date(2025, 10, 22), "S3")


def test_bucket_timestamps_are_truncated_to_whole_seconds(spark):
    frames = _frames(spark, [
        _bucket("2025-10-23 13:00:00.900", "2025-10-23 13:15:00.999")])
    row = _stage(frames).first()
    assert row.bucket_minutes == 15
    assert row.production_day == date(2025, 10, 23)


def test_timezone_conversion_and_dst_per_plant(spark):
    plants = (("PLT01", CHI), ("PLT02", "Central Standard Time (Mexico)"),
              ("PLT03", "GMT Standard Time"))
    patterns = {"PLT03": (("D", "06:00", "18:00", 0),
                          ("N", "18:00", "06:00", 1))}
    buckets = [
        _bucket("2025-10-20 13:00:00", line="PLT01-L1", sku="CDT"),
        _bucket("2025-11-10 11:30:00", line="PLT01-L1", sku="CST"),
        _bucket("2025-10-20 11:30:00", line="PLT02-L1", sku="MEX"),
        _bucket("2025-10-25 05:30:00", line="PLT03-L1", sku="BST"),
        _bucket("2025-10-27 05:30:00", line="PLT03-L1", sku="GMT"),
    ]
    frames = _frames(spark, buckets, plants=plants, patterns=patterns,
                     days=("2025-10-19", "2025-10-20", "2025-10-25", "2025-10-26",
                           "2025-11-09"))
    got = {(r.line_id, r.sku_id): (r.production_day, r.shift_code)
           for r in _stage(frames).collect()}
    assert got == {
        ("PLT01-L1", "CDT"):
            (date(2025, 10, 20), "S1"),
        ("PLT01-L1", "CST"):
            (date(2025, 11, 9), "S3"),
        ("PLT02-L1", "MEX"):
            (date(2025, 10, 19), "S3"),
        ("PLT03-L1", "BST"):
            (date(2025, 10, 25), "D"),
        ("PLT03-L1", "GMT"):
            (date(2025, 10, 26), "N"),
    }


def test_fall_back_fold_buckets_both_match_prior_day_s3(spark):
    frames = _frames(spark, [
        _bucket("2025-11-02 06:30:00"),
        _bucket("2025-11-02 07:30:00"),
    ], days=("2025-11-01",))
    rows = _stage(frames).collect()
    assert [(r.production_day, r.shift_code) for r in rows] == [
        (date(2025, 11, 1), "S3"), (date(2025, 11, 1), "S3")]


def test_bucket_minutes_are_counted_on_utc_minute_boundaries(spark):
    frames = _frames(spark, [
        _bucket("2025-11-02 06:45:00", "2025-11-02 08:00:00"),
        _bucket("2025-11-02 08:00:00", "2025-11-02 08:15:00"),
        _bucket("2025-11-02 10:00:59", "2025-11-02 10:01:00"),
    ], days=("2025-11-01",))
    assert sorted(row.bucket_minutes for row in _stage(frames).collect()) == [1, 15, 75]


def test_shift_boundary_is_half_open(spark):
    frames = _frames(spark, [
        _bucket("2025-10-23 18:45:00", sku="BEFORE"),  # 13:45 local
        _bucket("2025-10-23 19:00:00", sku="AT"),      # 14:00 local
    ])
    got = {row.sku_id: row.shift_code for row in _stage(frames).collect()}
    assert got == {"BEFORE": "S1", "AT": "S2"}


def test_dropped_line_plant_and_shift_buckets_are_logged(spark, caplog):
    caplog.set_level(logging.INFO, logger=job.log.name)
    frames = _frames(
        spark,
        [_bucket("2025-10-23 13:00:00", line="UNKNOWN"),
         _bucket("2025-10-23 13:00:00", line="PLT01-L1"),
         _bucket("2025-10-23 02:00:00", line="PLT01-L2")],
        lines=(("PLT01-L1", "PLT09 "), ("PLT01-L2", "PLT01 ")),
    )
    got = _stage(frames).count()
    assert got == 0
    messages = [record.getMessage() for record in caplog.records]
    assert any("dropped 1 bucket(s) at dim.line" in msg and "UNKNOWN" in msg
               for msg in messages)
    assert any("dropped 1 bucket(s) at dim.plant" in msg and "PLT01-L1" in msg
               for msg in messages)
    assert any("dropped 1 bucket(s) at dim.shift_calendar" in msg
               for msg in messages)


def test_shift_calendar_fanout_is_logged_and_kept(spark, caplog):
    caplog.set_level(logging.WARNING, logger=job.log.name)
    frames = _frames(spark, [_bucket("2025-10-23 13:00:00")])
    plants = job.plants_with_iana(frames["dim.plant"])
    calendar = job.build_shift_calendar(
        plants, frames["dim.shift_pattern"], frames["dim.calendar"])
    duplicate_shift = calendar.where(F.col("shift_code") == "S1")
    calendar = calendar.unionByName(duplicate_shift)
    staged = job.stg_production_local(
        frames["mes.production_count"], frames["dim.line"], plants, calendar)
    assert staged.count() == 2
    assert any("matched 1 bucket(s) to multiple shift_calendar rows; keeping duplicates"
               in record.getMessage() for record in caplog.records)


def test_unmatched_line_and_sku_are_logged(spark, caplog):
    caplog.set_level(logging.INFO, logger=job.log.name)
    frames = _frames(spark, [
        _bucket("2025-10-23 13:00:00", line="UNKNOWN"),
        _bucket("2025-10-23 13:00:00", sku="MISSING"),
    ], skus=(("SKU01", 12),))
    result = job.build_report(frames, AS_OF)
    assert result.count() == 0
    messages = [record.getMessage() for record in caplog.records]
    assert any("dropped 1 bucket(s) at dim.line" in msg and "UNKNOWN" in msg
               for msg in messages)
    assert any("dropped 1 stage row(s) at dim.sku inner join" in msg
               and "MISSING" in msg for msg in messages)


def test_stage_trims_char_padded_ids_and_has_legacy_schema(spark):
    frames = _frames(spark, [
        _bucket("2025-10-23 13:00:00", sku="SKU01 "),
    ], lines=(("PLT01-L1", "PLT01 "),), skus=(("SKU01 ", 12),))
    stg = _stage(frames)
    assert stg.dtypes == [
        ("line_id", "string"), ("plant_id", "string"), ("sku_id", "string"),
        ("production_day", "date"), ("shift_code", "string"),
        ("bucket_minutes", "int"), ("total_units", "int"), ("good_units", "int")]
    assert stg.first().plant_id == "PLT01"
    assert stg.first().sku_id == "SKU01"
    assert job.build_report(frames, AS_OF).first().plant_id == "PLT01"


def test_grain_sums_buckets_and_keeps_distinct_skus(spark):
    frames = _frames(spark, [
        _bucket("2025-10-23 13:00:00", sku="SKU01", total=10, good=7),
        _bucket("2025-10-23 13:15:00", sku="SKU01", total=20, good=8),
        _bucket("2025-10-23 13:30:00", sku="SKU02", total=5, good=3),
    ], skus=(("SKU01", 5), ("SKU02", 2)))
    got = _result_map(job.build_report(frames, AS_OF))
    assert got == {
        ("PLT01", "PLT01-L1", "2025-10-23", "SKU01"):
            (30, 15, 3, Decimal("50.00")),
        ("PLT01", "PLT01-L1", "2025-10-23", "SKU02"):
            (5, 3, 1, Decimal("60.00")),
    }


@pytest.mark.parametrize("goods,pack_size,expected_cases", [
    ((35,), 12, 2),
    ((7, 8), 5, 3),
    ((-7,), 2, -3),
])
def test_cases_use_sum_then_integer_division_toward_zero(
        spark, goods, pack_size, expected_cases):
    frames = _frames(spark, [
        _bucket(f"2025-10-23 13:{minute:02d}:00", total=100, good=good)
        for minute, good in zip((0, 15), goods)
    ], skus=(("SKU01", pack_size),))
    assert job.build_report(frames, AS_OF).first().cases == expected_cases


@pytest.mark.parametrize("good,total,expected", [
    (1, 32, Decimal("3.13")),
    (201, 20000, Decimal("1.01")),
    (2, 3, Decimal("66.67")),
    (1, 3, Decimal("33.33")),
    (0, 10, Decimal("0.00")),
    (11, 10, Decimal("110.00")),
])
def test_yield_pct_decimal_rounding(spark, good, total, expected):
    frames = _frames(spark, [_bucket("2025-10-23 13:00:00", total=total, good=good)])
    assert job.build_report(frames, AS_OF).first().yield_pct == expected


def test_zero_total_yield_is_null_and_cases_zero(spark):
    frames = _frames(spark, [_bucket("2025-10-23 13:00:00", total=0, good=0)])
    row = job.build_report(frames, AS_OF).first()
    assert row.cases == 0
    assert row.yield_pct is None


def test_zero_pack_size_raises_sql_server_error(spark):
    frames = _frames(spark, [_bucket("2025-10-23 13:00:00")],
                     skus=(("SKU01", 0),))
    with pytest.raises(ZeroDivisionError, match="pack_size = 0.*SKU01"):
        job.build_report(frames, AS_OF).collect()


def test_integer_sum_overflow_raises(spark):
    frames = _frames(spark, [
        _bucket("2025-10-23 13:00:00", total=2_000_000_000, good=1),
        _bucket("2025-10-23 13:15:00", total=2_000_000_000, good=1),
    ])
    with pytest.raises(OverflowError, match="Arithmetic overflow converting expression"):
        job.build_report(frames, AS_OF).collect()


def test_as_of_is_validated_but_does_not_filter(spark):
    frames = _frames(spark, [
        _bucket("2025-11-16 13:00:00", "2025-11-16 13:15:00")],
        days=("2025-11-16",))
    late = job.build_report(frames, "2025-11-17 00:00:00")
    early = job.build_report(frames, "2025-10-01 00:00:00")
    assert late.exceptAll(early).count() == 0
    assert early.exceptAll(late).count() == 0
    assert early.count() == 1
    with pytest.raises(ValueError, match="unparseable --as-of-utc"):
        job.build_report(frames, "yesterday")


def test_unmapped_plant_timezone_raises(spark):
    frames = _frames(
        spark, [_bucket("2025-10-23 13:00:00")],
        plants=(("PLT01", "Mars Standard Time"),))
    with pytest.raises(ValueError, match="Mars Standard Time"):
        job.build_report(frames, AS_OF)


def test_report_schema_matches_legacy_types(spark):
    frames = _frames(spark, [_bucket("2025-10-23 13:00:00")])
    report = job.build_report(frames, AS_OF)
    assert report.dtypes == [
        ("plant_id", "string"), ("line_id", "string"), ("production_day", "date"),
        ("sku_id", "string"), ("total_units", "int"), ("good_units", "int"),
        ("cases", "int"), ("yield_pct", "decimal(9,2)")]


def test_full_seed_stage_count_and_totals(spark):
    frames = {table: read_raw(spark, table) for table in job.SOURCES}
    raw = frames["mes.production_count"]
    bucket_key = F.concat_ws(
        "|", *[F.col(column).cast("string") for column in (
            "line_id", "bucket_start_utc", "bucket_end_utc", "sku_id",
            "total_units", "good_units")])
    frames["mes.production_count"] = raw.withColumn("sku_id", bucket_key)
    stage = _stage(frames)
    assert raw.count() == stage.count() == 61336
    assert raw.agg(F.sum(F.col("total_units").cast("long"))).first()[0] == \
        stage.agg(F.sum("total_units")).first()[0]
    assert raw.agg(F.sum(F.col("good_units").cast("long"))).first()[0] == \
        stage.agg(F.sum("good_units")).first()[0]
    assert stage.groupBy("sku_id").count().where("count != 1").count() == 0
    durations = {row.bucket_minutes: row["count"] for row in
                 stage.groupBy("bucket_minutes").count().collect()}
    assert durations == {15: 61321, 75: 15}


def test_job_and_shared_helpers_have_no_wall_clock_calls():
    root = Path(__file__).resolve().parents[1] / "lakehouse" / "src" / "mfg_lake"
    sources = [
        root / "jobs" / "daily_production.py",
        root / "common" / "timeconv.py",
        root / "common" / "shift_calendar.py",
    ]
    forbidden = ("current_timestamp", "now(", "current_date", "time.time(")
    for path in sources:
        text = path.read_text()
        assert not any(token in text for token in forbidden), path
