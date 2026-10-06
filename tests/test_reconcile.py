import csv
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import reconcile  # noqa: E402


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Synthetic mini-estate: one report with two key columns."""
    snap = tmp_path / "snap"
    snap.mkdir()
    (snap / "rpt.t.csv").write_text(
        "k1,k2,val,label\n"
        "a,1,1.50,foo\n"
        "b,2,2.25,bar\n")
    cfg = {"reports": {"t": {"table": "rpt.t", "keys": ["k1", "k2"]}},
           "numeric_tolerance": 1e-6, "sample_diffs": 10}
    monkeypatch.setattr(reconcile, "load_snapshot",
                        lambda table: pd.read_csv(snap / f"{table}.csv",
                                                  dtype=str, keep_default_na=False))
    monkeypatch.setattr(reconcile, "reconciliation_dir",
                        lambda ns: tmp_path / ns / "reconciliation")
    monkeypatch.setattr(reconcile, "curated_dir",
                        lambda report, ns: tmp_path / ns / "curated" / report)
    return cfg, tmp_path


def _write_lake(tmp_path, rows):
    d = tmp_path / "ns1" / "curated" / "t"
    d.mkdir(parents=True)
    pd.DataFrame(rows, columns=["k1", "k2", "val", "label"]).to_parquet(d / "part-0.parquet")


def test_pass(env):
    cfg, tmp = env
    _write_lake(tmp, [("a", 1, 1.5, "foo"), ("b", 2, 2.25, "bar")])
    assert reconcile.reconcile("t", "ns1", cfg)


def test_numeric_within_tolerance(env):
    cfg, tmp = env
    _write_lake(tmp, [("a", 1, 1.5000005, "foo"), ("b", 2, 2.25, "bar")])
    assert reconcile.reconcile("t", "ns1", cfg)


def test_fail_on_value(env):
    cfg, tmp = env
    _write_lake(tmp, [("a", 1, 9.99, "foo"), ("b", 2, 2.25, "bar")])
    assert not reconcile.reconcile("t", "ns1", cfg)


def test_fail_on_missing_key(env):
    cfg, tmp = env
    _write_lake(tmp, [("a", 1, 1.5, "foo")])
    assert not reconcile.reconcile("t", "ns1", cfg)


def test_fail_on_extra_row(env):
    cfg, tmp = env
    _write_lake(tmp, [("a", 1, 1.5, "foo"), ("b", 2, 2.25, "bar"), ("c", 3, 1, "x")])
    assert not reconcile.reconcile("t", "ns1", cfg)


def test_boolean_bit_column_matches_0_1(tmp_path, monkeypatch):
    """BIT exports as 0/1; boolean lake columns must reconcile (incl. as keys)."""
    snap = tmp_path / "snap"
    snap.mkdir()
    (snap / "rpt.b.csv").write_text("k,flag,n\na,0,1\na,1,2\n")
    cfg = {"reports": {"b": {"table": "rpt.b", "keys": ["k", "flag"]}}}
    monkeypatch.setattr(reconcile, "load_snapshot",
                        lambda table: pd.read_csv(snap / f"{table}.csv",
                                                  dtype=str, keep_default_na=False))
    monkeypatch.setattr(reconcile, "reconciliation_dir",
                        lambda ns: tmp_path / ns / "reconciliation")
    monkeypatch.setattr(reconcile, "curated_dir",
                        lambda report, ns: tmp_path / ns / "curated" / report)
    d = tmp_path / "ns1" / "curated" / "b"
    d.mkdir(parents=True)
    pd.DataFrame({"k": ["a", "a"], "flag": [False, True], "n": [1, 2]}).to_parquet(
        d / "part-0.parquet")
    assert reconcile.reconcile("b", "ns1", cfg)
    pd.DataFrame({"k": ["a", "a"], "flag": [False, False], "n": [1, 2]}).to_parquet(
        d / "part-0.parquet")
    assert not reconcile.reconcile("b", "ns1", cfg)


def test_decimal_column_and_date_key_match_snapshot_strings(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from datetime import date
    from decimal import Decimal

    snap = tmp_path / "snap"
    snap.mkdir()
    (snap / "rpt.decimal.csv").write_text(
        "production_day,yield_pct\n2025-10-23,98.20\n2025-10-24,\n")
    cfg = {"reports": {"decimal": {
        "table": "rpt.decimal", "keys": ["production_day"]}}}
    monkeypatch.setattr(reconcile, "load_snapshot",
                        lambda table: pd.read_csv(snap / f"{table}.csv",
                                                  dtype=str, keep_default_na=False))
    monkeypatch.setattr(reconcile, "reconciliation_dir",
                        lambda ns: tmp_path / ns / "reconciliation")
    monkeypatch.setattr(reconcile, "curated_dir",
                        lambda report, ns: tmp_path / ns / "curated" / report)
    d = tmp_path / "ns1" / "curated" / "decimal"
    d.mkdir(parents=True)

    def write(values):
        table = pa.table({
            "production_day": pa.array(
                [date(2025, 10, 23), date(2025, 10, 24)], type=pa.date32()),
            "yield_pct": pa.array(values, type=pa.decimal128(9, 2)),
        })
        pq.write_table(table, d / "part-0.parquet")

    write([Decimal("98.20"), None])
    assert reconcile.reconcile("decimal", "ns1", cfg)
    write([Decimal("98.21"), None])
    assert not reconcile.reconcile("decimal", "ns1", cfg)
