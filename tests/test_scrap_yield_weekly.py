"""PL_Scrap_Yield conversion: SQL Server behaviours that must survive.

Written before mfg_lake.jobs.scrap_yield_weekly; each test pins one legacy
behaviour from mes.usp_allocate_scrap_to_orders /
rpt.usp_rpt_scrap_yield_weekly (plus the KAN-6 staging it inherits). The
independent oracle is tools/oracle_scrap_yield_weekly.py (a literal port of
the alloc cursor + the rpt CTEs).
"""
import csv
import inspect
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from mfg_lake.common.spark import get_spark
from mfg_lake.jobs import daily_production
from mfg_lake.jobs import scrap_yield_weekly as job

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import oracle_scrap_yield_weekly as oracle  # noqa: E402
from fuzz_daily_production import Dims  # noqa: E402

RAW = ROOT / "data" / "raw"
TS = "%Y-%m-%d %H:%M:%S"

PLANTS = [("PLT01", "Neenah Tissue", "Central Standard Time", "3x8")]
LINES = [("PLT01-L1", "PLT01", "Line 1")]
PATTERNS = [
    ("PLT01", "S1", "06:00", "14:00", 0), ("PLT01", "S2", "14:00", "22:00", 0),
    ("PLT01", "S3", "22:00", "06:00", 1),
]
CAL_START, CAL_DAYS = date(2025, 10, 20), 28
T_TS = "2025-10-21 12:00:00"  # default scrap timestamp for alloc tests


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test_scrap_yield_weekly")
    yield s
    s.stop()


# ------------------------------------------------------------------- feeds
def _df(spark, rows, schema):
    return spark.createDataFrame(list(rows), schema)


def _scrap_df(spark, rows):
    """rows: (scrap_id, line_id, ts_str, qty, type)"""
    return _df(spark, rows,
               "scrap_id int, line_id string, scrap_ts_utc string, "
               "qty_units int, scrap_type string")


def _order_df(spark, rows):
    """rows: (order_id, line_id, plant_id, sku_id, start_str, end_str, planned_qty)"""
    return _df(spark, rows,
               "order_id string, line_id string, plant_id string, sku_id string, "
               "sched_start_utc string, sched_end_utc string, planned_qty int")


def _o(order_id, line, start, end, plant="PLT01", sku="SKU-A", qty=1000):
    return (order_id, line, plant, sku, start, end, qty)


def _alloc(spark, scraps, orders):
    """{order_id: qty} per single-scrap call."""
    df = job.allocate_scrap_to_orders(_scrap_df(spark, scraps), _order_df(spark, orders))
    return {(r.scrap_id, r.order_id): r.qty_units for r in df.collect()}


def _s(scrap_id=1, line="PLT01-L1", ts=T_TS, qty=100, typ="contamination"):
    return (scrap_id, line, ts, qty, typ)


def _bucket(line, sku, start_utc, total, good, minutes=15):
    s = datetime.fromisoformat(start_utc)
    return (line, sku, s.strftime(TS),
            (s + timedelta(minutes=minutes)).strftime(TS), total, good)


def _feeds(spark, counts=(), scraps=(), orders=(), lines=LINES,
           cal_start=CAL_START, cal_days=CAL_DAYS):
    cal = [((cal_start + timedelta(days=i)).isoformat(), 0, 0, "")
           for i in range(cal_days)]
    pc = [(i + 1, *r) for i, r in enumerate(counts)]
    return {
        "dim.plant": _df(spark, PLANTS,
                         "plant_id string, plant_name string, tz_name string, shift_pattern string"),
        "dim.line": _df(spark, lines, "line_id string, plant_id string, line_name string"),
        "dim.shift_pattern": _df(
            spark, PATTERNS,
            "plant_id string, shift_code string, local_start string, local_end string, end_next_day int"),
        "dim.calendar": _df(spark, cal,
                            "calendar_date string, iso_week int, day_of_week int, day_name string"),
        "mes.production_count": _df(
            spark, pc,
            "bucket_id int, line_id string, sku_id string, bucket_start_utc string, "
            "bucket_end_utc string, total_units int, good_units int"),
        "mes.scrap_event": _scrap_df(spark, scraps),
        "mes.production_order": _order_df(spark, orders),
    }


def _run(spark, counts=(), scraps=(), **kw):
    """transform -> {(plant, line, iso_year, iso_week): row}"""
    df = job.transform(_feeds(spark, counts=counts, scraps=scraps, **kw))
    return {(r.plant_id, r.line_id, r.iso_year, r.iso_week): r for r in df.collect()}


def _raw_rows(name):
    with open(RAW / f"{name}.csv", newline="") as f:
        return list(csv.DictReader(f))


# ------------------------------------------------- allocate_scrap_to_orders
def test_alloc_proportional_remainder_to_largest(spark):
    # 10800/9000(clipped)/1800 = 21600 -> floors 50,41,8; remainder 1 -> O1
    out = _alloc(spark, [_s(qty=100)],
                 [_o("O1", "PLT01-L1", "2025-10-21 10:00:00", "2025-10-21 13:00:00"),
                  _o("O2", "PLT01-L1", "2025-10-21 11:30:00", "2025-10-21 20:00:00"),
                  _o("O3", "PLT01-L1", "2025-10-21 12:00:00", "2025-10-21 12:30:00")])
    assert out == {(1, "O1"): 51, (1, "O2"): 41, (1, "O3"): 8}


def test_alloc_tie_broken_by_order_id_string(spark):
    # equal overlaps: TOP 1 ORDER BY ov_secs DESC, order_id -> 'ORD-10' < 'ORD-2'
    out = _alloc(spark, [_s(qty=7)],
                 [_o("ORD-2", "PLT01-L1", "2025-10-21 11:00:00", "2025-10-21 13:00:00"),
                  _o("ORD-10", "PLT01-L1", "2025-10-21 11:00:00", "2025-10-21 13:00:00")])
    assert out == {(1, "ORD-10"): 4, (1, "ORD-2"): 3}


def test_alloc_window_clipped_to_7200s(spark):
    # O1 saturates at the +-2h window (14400s), O2 is fully inside (3600s)
    out = _alloc(spark, [_s(qty=10)],
                 [_o("O1", "PLT01-L1", "2025-10-21 00:00:00", "2025-10-21 23:00:00"),
                  _o("O2", "PLT01-L1", "2025-10-21 11:00:00", "2025-10-21 12:00:00")])
    assert out == {(1, "O1"): 8, (1, "O2"): 2}


def test_alloc_between_inclusive(spark):
    # start == ts and end == ts both count; end == ts-1s does not
    out = _alloc(spark, [_s(qty=5)],
                 [_o("ORD-A", "PLT01-L1", "2025-10-21 10:00:00", "2025-10-21 12:00:00"),
                  _o("ORD-B", "PLT01-L1", "2025-10-21 12:00:00", "2025-10-21 15:00:00"),
                  _o("ORD-C", "PLT01-L1", "2025-10-21 10:00:00", "2025-10-21 11:59:59")])
    assert out == {(1, "ORD-A"): 3, (1, "ORD-B"): 2}


def test_alloc_remainder_not_spread(spark):
    # qty=2 over three equal overlaps: floors 0,0,0; all remainder to the
    # TOP-1 order only (X1); zero-qty rows are still emitted
    out = _alloc(spark, [_s(qty=2)],
                 [_o("X1", "PLT01-L1", "2025-10-21 11:00:00", "2025-10-21 13:00:00"),
                  _o("X2", "PLT01-L1", "2025-10-21 11:00:00", "2025-10-21 13:00:00"),
                  _o("X3", "PLT01-L1", "2025-10-21 11:00:00", "2025-10-21 13:00:00")])
    assert out == {(1, "X1"): 2, (1, "X2"): 0, (1, "X3"): 0}


def test_alloc_other_line_ignored_unallocated(spark):
    out = _alloc(spark, [_s(qty=9)],
                 [_o("O1", "PLT01-L2", "2025-10-21 11:00:00", "2025-10-21 13:00:00")])
    assert out == {(1, "UNALLOCATED"): 9}


def test_alloc_zero_total_raises(spark):
    # zero-length order at ts is a candidate (BETWEEN inclusive) -> tot = 0
    with pytest.raises(ZeroDivisionError):
        _alloc(spark, [_s(qty=5)],
               [_o("O1", "PLT01-L1", "2025-10-21 12:00:00", "2025-10-21 12:00:00")])


def test_alloc_int_overflow_raises(spark):
    # 200000 * 14400 overflows INT -> SQL Server arithmetic overflow
    with pytest.raises(ArithmeticError):
        _alloc(spark, [_s(qty=200000)],
               [_o("O1", "PLT01-L1", "2025-10-21 10:00:00", "2025-10-21 14:00:00")])


def test_alloc_matches_cursor_port_on_seed(spark):
    scraps, orders = _raw_rows("mes.scrap_event"), _raw_rows("mes.production_order")
    feeds = _feeds(spark, scraps=[(int(r["scrap_id"]), r["line_id"], r["scrap_ts_utc"],
                                 int(r["qty_units"]), r["scrap_type"]) for r in scraps],
                   orders=[(r["order_id"], r["line_id"], r["plant_id"], r["sku_id"],
                            r["sched_start_utc"], r["sched_end_utc"], int(r["planned_qty"]))
                           for r in orders])
    got = Counter((r.scrap_id, r.order_id, r.qty_units)
                  for r in job.allocate_scrap_to_orders(
                      feeds["mes.scrap_event"], feeds["mes.production_order"]).collect())
    exp = Counter(oracle.cursor_alloc(scraps, orders))
    assert got == exp
    # conservation: per scrap_id the allocation sums back to qty_units
    by_scrap = Counter()
    for sid, _, q in exp:
        by_scrap[sid] += q
    valid = [r for r in scraps if all(r[c] for c in oracle.SE_NOT_NULL)]
    assert dict(by_scrap) == {int(r["scrap_id"]): int(r["qty_units"]) for r in valid}


# ------------------------------------------------------------ ISO calendar
def test_scrap_week_uses_utc(spark):
    # scrap 2025-10-27 03:00 UTC is local Sun 2025-10-26 22:00 CDT:
    # ISO week of the UTC ts -> 44; a local-week port would land on 43
    out = _run(spark,
               counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", 10, 9),
                       _bucket("PLT01-L1", "SKU-A", "2025-10-27 12:00:00", 10, 9)],
               scraps=[_s(ts="2025-10-27 03:00:00", qty=5)])
    w43 = out[("PLT01", "PLT01-L1", 2025, 43)]
    w44 = out[("PLT01", "PLT01-L1", 2025, 44)]
    assert w43.scrap_units == 0 and w44.scrap_units == 5


@pytest.mark.parametrize("ts,expected", [
    ("2024-12-29 23:59:59", (2024, 52)),
    ("2024-12-30 00:00:00", (2025, 1)),
    ("2027-01-01 00:00:00", (2026, 53)),
    ("2021-01-03 00:00:00", (2020, 53)),
    ("2026-01-01 00:00:00", (2026, 1)),
    ("2022-01-01 00:00:00", (2021, 52)),
    ("2025-12-29 00:00:00", (2026, 1)),
])
def test_iso_year_week_around_jan_1(spark, ts, expected):
    df = _df(spark, [(ts,)], "s string").select(
        job.iso_year_week(F.to_timestamp(F.col("s"))).alias("w"))
    r = df.select("w.*").collect()[0]
    assert (r.iso_year, r.iso_week) == expected


def test_production_day_monday_0600(spark):
    # Chicago CDT: Mon 05:30 local = 10:30Z -> production_day Sun 10-26 (wk43);
    # 06:00 local = 11:00Z -> day 10-27 (wk44)
    out = _run(spark,
               counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-27 10:30:00", 10, 3),
                       _bucket("PLT01-L1", "SKU-A", "2025-10-27 11:00:00", 10, 7)])
    assert out[("PLT01", "PLT01-L1", 2025, 43)].good_units == 3
    assert out[("PLT01", "PLT01-L1", 2025, 44)].good_units == 7


# -------------------------------------------------------------- report rows
def test_left_join_drops_scrap_only_week(spark):
    # scrap in week 44 with no week-44 production: good LEFT JOIN scrap drops it
    out = _run(spark,
               counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", 10, 9)],
               scraps=[_s(ts="2025-10-27 03:00:00", qty=5)])
    assert set(out) == {("PLT01", "PLT01-L1", 2025, 43)}


def test_week_without_scrap_reports_zero(spark):
    out = _run(spark, counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", 10, 9)])
    r = out[("PLT01", "PLT01-L1", 2025, 43)]
    assert r.scrap_units == 0 and r.scrap_pct == Decimal("0.00")


def test_unknown_line_scrap_dropped(spark):
    out = _run(spark,
               counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", 10, 9)],
               scraps=[_s(line="PLT99-L9", ts="2025-10-21 12:00:00", qty=5)])
    assert out[("PLT01", "PLT01-L1", 2025, 43)].scrap_units == 0


@pytest.mark.parametrize("good,scrap,expected", [
    (799, 1, Decimal("0.13")),      # 0.125 exact tie -> away from zero (banker's: 0.12)
    (17531, 2469, Decimal("12.35")),  # 12.345 decimal -> 12.35 (double math gives 12.34)
])
def test_scrap_pct_rounding(spark, good, scrap, expected):
    out = _run(spark,
               counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", good, good)],
               scraps=[_s(ts="2025-10-21 12:00:00", qty=scrap)])
    assert out[("PLT01", "PLT01-L1", 2025, 43)].scrap_pct == expected


def test_scrap_pct_null_when_den_zero(spark):
    out = _run(spark, counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", 0, 0)])
    assert out[("PLT01", "PLT01-L1", 2025, 43)].scrap_pct is None


def test_scrap_pct_100_when_no_good(spark):
    out = _run(spark,
               counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", 0, 0)],
               scraps=[_s(ts="2025-10-21 12:00:00", qty=5)])
    assert out[("PLT01", "PLT01-L1", 2025, 43)].scrap_pct == Decimal("100.00")


def test_good_int_overflow_raises(spark):
    with pytest.raises(ArithmeticError):
        _run(spark, counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00",
                                    2_000_000_000, 2_000_000_000),
                            _bucket("PLT01-L1", "SKU-A", "2025-10-20 12:15:00",
                                    2_000_000_000, 2_000_000_000)])


def test_unknown_sku_counts_toward_good(spark):
    # stg.production_local has no dim.sku join; unknown skus still count
    out = _run(spark, counts=[_bucket("PLT01-L1", "SKU-ZZ9", "2025-10-20 12:00:00", 10, 9)])
    assert out[("PLT01", "PLT01-L1", 2025, 43)].good_units == 9


def test_duplicates_are_summed_like_legacy(spark):
    b = _bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", 10, 9)
    out = _run(spark, counts=[b, b],
               scraps=[_s(1, ts="2025-10-21 12:00:00", qty=5),
                       _s(2, ts="2025-10-21 12:00:00", qty=5)])
    r = out[("PLT01", "PLT01-L1", 2025, 43)]
    assert r.good_units == 18 and r.scrap_units == 10


def test_scrap_not_null_rows_dropped(spark):
    out = _run(spark,
               counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", 10, 9)],
               scraps=[(1, None, "2025-10-21 12:00:00", 5, "x"),
                       (2, "PLT01-L1", None, 5, "x"),
                       (3, "PLT01-L1", "2025-10-21 12:00:00", None, "x"),
                       (4, "PLT01-L1", "2025-10-21 12:00:00", 7, "x")])
    assert out[("PLT01", "PLT01-L1", 2025, 43)].scrap_units == 7


def test_output_schema_matches_rpt_scrap_yield_weekly(spark):
    # dim.line.plant_id is CHAR(5): 'PLT01 ' padded input comes out trimmed
    f = _feeds(spark, counts=[_bucket("PLT01-L1", "SKU-A", "2025-10-20 12:00:00", 10, 9)],
               lines=[("PLT01-L1", "PLT01 ", "Line 1")])
    df = job.transform(f)
    assert [(c.name, c.dataType) for c in df.schema.fields] == [
        ("plant_id", T.StringType()), ("line_id", T.StringType()),
        ("iso_year", T.IntegerType()), ("iso_week", T.IntegerType()),
        ("good_units", T.IntegerType()), ("scrap_units", T.IntegerType()),
        ("scrap_pct", T.DecimalType(9, 2)),
    ]
    assert df.collect()[0].plant_id == "PLT01"


def test_transform_matches_oracle_on_seed(spark):
    from mfg_lake.common.io import read_raw
    feeds = {t: read_raw(spark, t) for t in job.FEEDS}
    got = {(r.plant_id, r.line_id, r.iso_year, r.iso_week):
           (r.good_units, r.scrap_units, r.scrap_pct)
           for r in job.transform(feeds).collect()}
    exp = oracle.report(Dims(RAW), _raw_rows("mes.production_count"),
                        _raw_rows("mes.scrap_event"))
    assert set(got) == set(exp)
    for k in exp:
        g, s, p = got[k]
        assert (g, s, p) == (exp[k][0], exp[k][1],
                             None if exp[k][2] is None else Decimal(exp[k][2]))


# ---------------------------------------------------------------------- CLI
def test_cli_requires_ns_and_as_of_utc():
    with pytest.raises(SystemExit):
        job.parse_args(["--ns", "x"])
    with pytest.raises(SystemExit):
        job.parse_args(["--as-of-utc", "2025-11-17 00:00:00"])
    a = job.parse_args(["--ns", "x", "--as-of-utc", "2025-11-17 00:00:00"])
    assert (a.ns, a.as_of_utc) == ("x", "2025-11-17 00:00:00")


def test_cli_rejects_malformed_as_of_utc():
    for bad in ["2025-11-17", "17/11/2025 00:00:00", "2025-11-17 25:00:00", "now"]:
        with pytest.raises(SystemExit):
            job.parse_args(["--ns", "x", "--as-of-utc", bad])


def _write_raw(dir_):
    """Minimal raw feeds: one plant/line/pattern, 2 calendar days."""
    dir_.mkdir(parents=True, exist_ok=True)

    def w(name, header, rows):
        with open(dir_ / f"{name}.csv", "w", newline="") as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(rows)
    w("dim.plant", ["plant_id", "plant_name", "tz_name", "shift_pattern"], PLANTS)
    w("dim.line", ["line_id", "plant_id", "line_name"], LINES)
    w("dim.shift_pattern",
      ["plant_id", "shift_code", "local_start", "local_end", "end_next_day"], PATTERNS)
    w("dim.calendar", ["calendar_date", "iso_week", "day_of_week", "day_name"],
      [("2025-10-20", 43, 1, "Monday"), ("2025-10-21", 43, 2, "Tuesday")])
    w("mes.production_count",
      ["bucket_id", "line_id", "sku_id", "bucket_start_utc", "bucket_end_utc",
       "total_units", "good_units"],
      [(1, "PLT01-L1", "SKU-A", "2025-10-20 12:00:00", "2025-10-20 12:15:00", 10, 9)])
    w("mes.scrap_event", ["scrap_id", "line_id", "scrap_ts_utc", "qty_units", "scrap_type"],
      [(1, "PLT01-L1", "2025-10-21 12:00:00", 5, "x")])
    w("mes.production_order",
      ["order_id", "line_id", "plant_id", "sku_id", "sched_start_utc",
       "sched_end_utc", "planned_qty"],
      [("O1", "PLT01-L1", "PLT01", "SKU-A", "2025-10-21 11:00:00",
        "2025-10-21 13:00:00", 100)])


def test_run_earlier_cutoff_identical_output(spark, tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    import mfg_lake.common.paths as paths
    _write_raw(tmp_path / "raw")
    monkeypatch.setenv("RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(paths, "LAKE_ROOT", tmp_path / "lake")
    rows = {}
    for cutoff, ns in [("2025-11-17 00:00:00", "a"), ("2025-11-03 00:00:00", "b")]:
        job.run(ns, cutoff, spark)
        d = paths.curated_dir("scrap_yield_weekly", ns)
        rows[ns] = pq.read_table(sorted(d.glob("*.parquet"))).to_pandas()
    assert rows["a"].equals(rows["b"])


# ------------------------------------------------------------- source audit
def test_source_uses_shared_helpers_and_no_wall_clock():
    src = inspect.getsource(job)
    for banned in ("current_timestamp", "current_date", "now(",
                   "datetime.now", "time.time"):
        assert banned not in src, banned
    assert job.stage_production_local is daily_production.stage_production_local
    assert job.build_shift_calendar is daily_production.build_shift_calendar
    assert job.plant_zones is daily_production.plant_zones
