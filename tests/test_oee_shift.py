import ast
import inspect
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DecimalType, IntegerType, StringType)

from mfg_lake.common.spark import get_spark
from mfg_lake.jobs import daily_production, oee_shift as job


@pytest.fixture(scope="module")
def spark():
    session = get_spark("test_oee_shift")
    yield session
    session.stop()


def _calendar(spark, rows):
    return spark.createDataFrame(
        rows,
        "plant_id string, shift_code string, production_day date, "
        "start_utc timestamp, end_utc timestamp",
    )


def _line(spark, rows):
    return spark.createDataFrame(rows, "line_id string, plant_id string")


def _segments(spark, rows):
    return spark.createDataFrame(
        rows,
        "plant_id string, line_id string, production_day date, shift_code string, "
        "planned_flag boolean, seg_start_local timestamp, seg_end_local timestamp",
    )


def _planned(spark, rows):
    return spark.createDataFrame(
        rows,
        "plant_id string, line_id string, production_day date, shift_code string, "
        "shift_minutes long, planned_dt_min long, unplanned_dt_min long",
    )


def _production(spark, rows):
    return spark.createDataFrame(
        rows,
        "plant_id string, line_id string, production_day date, shift_code string, "
        "sku_id string, bucket_minutes int, total_units long, good_units long",
    )


def _sku(spark, rows):
    return spark.createDataFrame(rows, "sku_id string, ideal_units_per_min decimal(9,2)")


def _report_row(spark, *, shift=32, unplanned=0, total=1, good=1, rate="1.00",
                bucket_minutes=1, day=date(2025, 10, 20), sku_id="SKU"):
    key = ("PLT01", "PLT01-L1", day, "S1")
    planned = _planned(spark, [(*key, shift, 0, unplanned)])
    production = _production(
        spark, [(*key, sku_id, bucket_minutes, total, good)])
    sku = _sku(spark, [(sku_id, Decimal(rate))])
    out = job.rpt_oee_shift(planned, production, sku).collect()
    return out[0] if out else None


def test_planned_and_unplanned_downtime_are_separate(spark):
    key = ("PLT01", "PLT01-L1", date(2025, 10, 20), "S1")
    calendar = _calendar(spark, [
        ("PLT01", "S1", key[2], datetime(2025, 10, 20, 8), datetime(2025, 10, 20, 16)),
    ])
    line = _line(spark, [("PLT01-L1", "PLT01")])
    segments = _segments(spark, [
        (*key, True, datetime(2025, 10, 20, 9), datetime(2025, 10, 20, 10, 30)),
        (*key, False, datetime(2025, 10, 20, 11), datetime(2025, 10, 20, 11, 30)),
    ])

    planned = job.calc_planned_time(calendar, line, segments).collect()[0]
    row = job.rpt_oee_shift(
        job.calc_planned_time(calendar, line, segments),
        _production(spark, [(*key, "SKU", 100, 100, 90)]),
        _sku(spark, [("SKU", Decimal("1.00"))]),
    ).collect()[0]

    assert (planned.shift_minutes, planned.planned_dt_min, planned.unplanned_dt_min) == (
        480, 90, 30)
    assert row.planned_min == 480
    assert row.unplanned_dt_min == 30
    assert row.availability == Decimal("0.9375")
    assert row.performance == Decimal("1.0000")
    assert row.quality == Decimal("0.9000")
    assert row.oee == Decimal("0.8438")


def test_shift_without_segments_has_zero_downtime(spark):
    day = date(2025, 10, 20)
    calendar = _calendar(spark, [
        ("PLT01", "S1", day, datetime(2025, 10, 20, 8), datetime(2025, 10, 20, 16)),
    ])
    planned = job.calc_planned_time(
        calendar, _line(spark, [("PLT01-L1", "PLT01")]),
        _segments(spark, []),
    ).collect()[0]
    assert (planned.planned_dt_min, planned.unplanned_dt_min) == (0, 0)


def test_segment_minutes_use_local_boundaries(spark):
    day = date(2025, 10, 20)
    key = ("PLT01", "PLT01-L1", day, "S1")
    calendar = _calendar(spark, [
        (*key[:1], key[3], day, datetime(2025, 10, 20, 8), datetime(2025, 10, 20, 16)),
    ])
    line = _line(spark, [("PLT01-L1", "PLT01")])
    segments = spark.createDataFrame(
        [
            (*key, False, datetime(2025, 10, 20, 10, 0, 59),
             datetime(2025, 10, 20, 10, 1), datetime(2025, 10, 20, 14),
             datetime(2025, 10, 20, 14, 1)),
            (*key, False, datetime(2025, 10, 20, 10),
             datetime(2025, 10, 20, 10, 2), datetime(2025, 10, 20, 14),
             datetime(2025, 10, 20, 17)),
        ],
        "plant_id string, line_id string, production_day date, shift_code string, "
        "planned_flag boolean, seg_start_local timestamp, seg_end_local timestamp, "
        "seg_start_utc timestamp, seg_end_utc timestamp",
    )

    planned = job.calc_planned_time(calendar, line, segments).collect()[0]
    assert planned.unplanned_dt_min == 3


def test_shift_minutes_use_utc_bounds(spark):
    row = _calendar(spark, [
        ("PLT01", "S1", date(2025, 10, 20),
         datetime(2025, 10, 20, 8, 0, 59), datetime(2025, 10, 20, 9, 1)),
    ])
    planned = job.calc_planned_time(
        row, _line(spark, [("PLT01-L1", "PLT01")]), _segments(spark, []),
    ).collect()[0]
    assert planned.shift_minutes == 61


def test_dst_shift_minutes_match_utc_duration(spark):
    plants = spark.createDataFrame(
        [("CEN01", "Central Standard Time"), ("LON01", "GMT Standard Time")],
        "plant_id string, tz_name string",
    )
    patterns = spark.createDataFrame(
        [("CEN01", "S3", "22:00", "06:00", 1),
         ("LON01", "N", "18:00", "06:00", 1)],
        "plant_id string, shift_code string, local_start string, local_end string, "
        "end_next_day int",
    )
    days = [date(2025, 11, 1), date(2026, 3, 7),
            date(2025, 10, 25), date(2026, 3, 28)]
    calendar_dates = spark.createDataFrame(
        [(day,) for day in days], "calendar_date date")
    calendar = job.build_shift_calendar(plants, patterns, calendar_dates)
    lines = _line(spark, [("CEN01-L1", "CEN01"), ("LON01-L1", "LON01")])
    planned = job.calc_planned_time(calendar, lines, _segments(spark, []))
    values = {(r.plant_id, r.production_day, r.shift_code): r.shift_minutes
              for r in planned.collect()}
    assert values[("CEN01", date(2025, 11, 1), "S3")] == 540
    assert values[("CEN01", date(2026, 3, 7), "S3")] == 420
    assert values[("LON01", date(2025, 10, 25), "N")] == 780
    assert values[("LON01", date(2026, 3, 28), "N")] == 660


def test_runtime_weighted_ideal(spark):
    day = date(2025, 10, 20)
    key = ("PLT01", "PLT01-L1", day, "S1")
    planned = _planned(spark, [(*key, 480, 0, 0)])
    production = _production(spark, [
        (*key, "FAST", 10, 100, 90),
        (*key, "SLOW", 20, 50, 40),
    ])
    sku = _sku(spark, [("FAST", Decimal("2.00")), ("SLOW", Decimal("1.00"))])
    row = job.rpt_oee_shift(planned, production, sku).collect()[0]
    # Ideal = 2*10 + 1*20 = 40; total = 150, good = 130.
    assert row.performance == Decimal("3.7500")
    assert row.quality == Decimal("0.8667")
    assert row.oee == Decimal("3.2500")


def test_nullif_guards(spark):
    no_units = _report_row(spark, total=0, good=0, rate="1.00", bucket_minutes=60)
    assert no_units.performance == Decimal("0.0000")
    assert no_units.quality is None
    assert no_units.oee is None

    no_ideal = _report_row(spark, total=10, good=8, rate="0.00", bucket_minutes=60)
    assert no_ideal.performance is None
    assert no_ideal.oee is None
    assert no_ideal.quality == Decimal("0.8000")

    no_shift = _report_row(spark, shift=0, unplanned=0, total=10, good=8,
                           rate="1.00", bucket_minutes=10)
    assert no_shift.availability is None
    assert no_shift.oee is None
    assert no_shift.performance == Decimal("1.0000")


def test_round_half_away_from_zero(spark):
    assert _report_row(spark, shift=32, unplanned=1).availability == Decimal("0.9688")
    assert _report_row(spark, total=20000, good=1, bucket_minutes=20000).quality == Decimal(
        "0.0001")
    assert _report_row(spark, shift=32, unplanned=33).availability == Decimal("-0.0313")


def test_oee_rounds_once_on_exact_product(spark):
    row = _report_row(spark, shift=1, unplanned=0, total=2, good=1,
                      rate="1.00", bucket_minutes=3)
    # Exact OEE is 1 * 2 * 1 / (1 * 3 * 2) = 1/3.
    assert row.availability == Decimal("1.0000")
    assert row.performance == Decimal("0.6667")
    assert row.quality == Decimal("0.5000")
    assert row.oee == Decimal("0.3333")
    assert (row.availability * row.performance * row.quality).quantize(
        Decimal("0.0001")) == Decimal("0.3334")


def test_snapshot_oee_rounding_regression(spark):
    row = _report_row(spark, shift=480, unplanned=0, total=80152, good=78929,
                      rate="190.00", bucket_minutes=480, day=date(2025, 10, 23))
    assert row.performance == Decimal("0.8789")
    assert row.quality == Decimal("0.9847")
    assert row.oee == Decimal("0.8654")
    rounded_components = (row.availability * row.performance * row.quality).quantize(
        Decimal("0.0001"))
    assert rounded_components == Decimal("0.8655")


def test_production_day_floor(spark):
    keys = [("PLT01", "PLT01-L1", date(2025, 10, 19), "S1"),
            ("PLT01", "PLT01-L1", date(2025, 10, 20), "S1")]
    planned = _planned(spark, [(*key, 480, 0, 0) for key in keys])
    production = _production(spark, [
        (*key, "SKU", 10, 10, 10) for key in keys])
    out = job.rpt_oee_shift(planned, production, _sku(spark, [("SKU", Decimal("1.00"))]))
    assert [row.production_day for row in out.collect()] == [date(2025, 10, 20)]


def test_calendar_without_line_is_dropped(spark):
    calendar = _calendar(spark, [
        ("PLT01", "S1", date(2025, 10, 20),
         datetime(2025, 10, 20, 8), datetime(2025, 10, 20, 16)),
    ])
    out = job.calc_planned_time(calendar, _line(spark, []), _segments(spark, []))
    assert out.count() == 0


def test_unknown_sku_is_dropped(spark):
    key = ("PLT01", "PLT01-L1", date(2025, 10, 20), "S1")
    out = job.rpt_oee_shift(
        _planned(spark, [(*key, 480, 0, 0)]),
        _production(spark, [(*key, "MISSING", 10, 10, 10)]),
        _sku(spark, []),
    )
    assert out.count() == 0


def test_planned_time_and_production_inner_join(spark):
    key1 = ("PLT01", "PLT01-L1", date(2025, 10, 20), "S1")
    key2 = ("PLT01", "PLT01-L1", date(2025, 10, 21), "S1")
    out = job.rpt_oee_shift(
        _planned(spark, [(*key1, 480, 0, 0)]),
        _production(spark, [(*key2, "SKU", 10, 10, 10)]),
        _sku(spark, [("SKU", Decimal("1.00"))]),
    )
    assert out.count() == 0


def test_duplicate_segments_and_buckets_are_summed(spark):
    day = date(2025, 10, 20)
    key = ("PLT01", "PLT01-L1", day, "S1")
    calendar = _calendar(spark, [
        ("PLT01", "S1", day, datetime(2025, 10, 20, 8), datetime(2025, 10, 20, 10)),
    ])
    segment = (*key, False, datetime(2025, 10, 20, 8), datetime(2025, 10, 20, 8, 5))
    planned = job.calc_planned_time(
        calendar, _line(spark, [("PLT01-L1", "PLT01")]),
        _segments(spark, [segment, segment]),
    )
    bucket = (*key, "SKU", 10, 30, 24)
    row = job.rpt_oee_shift(
        planned, _production(spark, [bucket, bucket]),
        _sku(spark, [("SKU", Decimal("1.00"))]),
    ).collect()[0]
    assert row.unplanned_dt_min == 10
    assert row.performance == Decimal("3.0000")
    assert row.quality == Decimal("0.8000")
    assert row.oee == Decimal("2.4000")


def test_unknown_line_is_dropped_by_imported_production_stage(spark):
    feeds = _transform_feeds(spark, line_rows=[])
    assert job.transform(feeds, "2025-10-20 15:00:00").count() == 0


def test_integer_overflow_raises(spark):
    key = ("PLT01", "PLT01-L1", date(2025, 10, 20), "S1")
    out = job.rpt_oee_shift(
        _planned(spark, [(*key, 480, 0, 0)]),
        _production(spark, [
            (*key, "SKU", 10, 2**31 - 1, 2**31 - 1),
            (*key, "SKU", 10, 1, 1),
        ]),
        _sku(spark, [("SKU", Decimal("1.00"))]),
    )
    with pytest.raises(Exception, match="overflow|INT"):
        out.collect()


def _transform_feeds(spark, *, line_rows=None, event_start="2025-10-20 13:00:00"):
    return {
        "dim.plant": spark.createDataFrame(
            [("PLT01", "Plant", "Central Standard Time", "3x8")],
            "plant_id string, plant_name string, tz_name string, shift_pattern string",
        ),
        "dim.line": spark.createDataFrame(
            line_rows if line_rows is not None else [("PLT01-L1", "PLT01", "Line")],
            "line_id string, plant_id string, line_name string",
        ),
        "dim.sku": _sku(spark, [("SKU", Decimal("1.00"))]),
        "dim.shift_pattern": spark.createDataFrame(
            [("PLT01", "S1", "06:00", "14:00", 0)],
            "plant_id string, shift_code string, local_start string, local_end string, "
            "end_next_day int",
        ),
        "dim.calendar": spark.createDataFrame(
            [("2025-10-20", 43, 1, "Monday")],
            "calendar_date string, iso_week int, day_of_week int, day_name string",
        ),
        "mes.production_count": spark.createDataFrame(
            [("PLT01-L1", "2025-10-20 12:00:00", "2025-10-20 12:10:00",
              "SKU", 100, 90)],
            "line_id string, bucket_start_utc string, bucket_end_utc string, "
            "sku_id string, total_units int, good_units int",
        ),
        "mes.downtime_event": spark.createDataFrame(
            [(1, "PLT01-L1", event_start, None, "R-JAM", 0)],
            "event_id int, line_id string, start_utc string, end_utc string, "
            "reason_code string, planned_flag int",
        ),
    }


def test_transform_cutoff_only_changes_open_downtime(spark):
    feeds = _transform_feeds(spark)
    early = job.transform(feeds, "2025-10-20 14:00:00").collect()[0]
    later = job.transform(feeds, "2025-10-20 15:00:00").collect()[0]
    assert early.unplanned_dt_min == 60
    assert later.unplanned_dt_min == 120
    assert early.planned_min == later.planned_min == 480


def test_transform_drops_pre_floor_and_unknown_line(spark):
    feeds = _transform_feeds(spark)
    feeds["dim.calendar"] = spark.createDataFrame(
        [("2025-10-19", 42, 7, "Sunday"), ("2025-10-20", 43, 1, "Monday")],
        "calendar_date string, iso_week int, day_of_week int, day_name string",
    )
    feeds["mes.production_count"] = spark.createDataFrame(
        [("PLT01-L1", "2025-10-19 12:00:00", "2025-10-19 12:10:00",
          "SKU", 100, 90),
         ("UNKNOWN", "2025-10-20 12:00:00", "2025-10-20 12:10:00",
          "SKU", 100, 90)],
        "line_id string, bucket_start_utc string, bucket_end_utc string, "
        "sku_id string, total_units int, good_units int",
    )
    out = job.transform(feeds, "2025-10-20 15:00:00").collect()
    assert [row.production_day for row in out] == [date(2025, 10, 20)]


def test_imported_stage_calls_and_source_contract():
    source = inspect.getsource(job)
    tree = ast.parse(source)
    definitions = {node.name for node in tree.body
                   if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    imports = {(node.module, alias.name) for node in tree.body
               if isinstance(node, ast.ImportFrom) for alias in node.names}
    assert ("mfg_lake.jobs.daily_production", "build_shift_calendar") in imports
    assert ("mfg_lake.jobs.daily_production", "stage_production_local") in imports
    for name in ("stage_downtime_local", "split_downtime_by_shift", "datediff_minute"):
        assert ("mfg_lake.jobs.line_downtime_daily", name) in imports
    assert not definitions.intersection({
        "build_shift_calendar", "stage_production_local", "stage_downtime_local",
        "split_downtime_by_shift", "datediff_minute",
    })
    called = {node.func.id for node in ast.walk(tree)
              if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
    assert {
        "build_shift_calendar", "stage_production_local", "stage_downtime_local",
        "split_downtime_by_shift", "datediff_minute",
    }.issubset(called)
    assert job.build_shift_calendar is daily_production.build_shift_calendar
    assert job.stage_production_local is daily_production.stage_production_local


def test_output_schema_matches_ddl_order_and_types(spark):
    row = _report_row(spark)
    assert row is not None
    schema = job.rpt_oee_shift(
        _planned(spark, [("PLT01", "PLT01-L1", date(2025, 10, 20), "S1", 32, 0, 0)]),
        _production(spark, [("PLT01", "PLT01-L1", date(2025, 10, 20), "S1",
                             "SKU", 1, 1, 1)]),
        _sku(spark, [("SKU", Decimal("1.00"))]),
    ).schema
    assert schema.fieldNames() == list(job.OUTPUT_COLUMNS)
    assert [field.dataType for field in schema] == [
        StringType(), StringType(), DateType(), StringType(), IntegerType(), IntegerType(),
        DecimalType(9, 4), DecimalType(9, 4), DecimalType(9, 4), DecimalType(9, 4),
    ]
    assert all(field.nullable for field in schema[-4:])


def test_cli_requires_and_parses_ns_and_as_of_utc():
    args = job.parse_args(["--ns", "dev", "--as-of-utc", "2025-11-17 00:00:00"])
    assert args.ns == "dev"
    assert args.as_of_utc == "2025-11-17 00:00:00"
    with pytest.raises(SystemExit):
        job.parse_args([])


def test_job_source_has_no_wall_clock_cutoff():
    source_path = Path(inspect.getsourcefile(job))
    source = source_path.read_text().lower()
    assert "current_timestamp" not in source
    assert "now(" not in source
    assert "datetime.now" not in source
