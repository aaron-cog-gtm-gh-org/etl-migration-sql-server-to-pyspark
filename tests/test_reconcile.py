import csv
import sys
from pathlib import Path

import numpy as np
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


# --------------------------------------------------------------- BIT columns

def _bit_env(env):
    """Report whose snapshot has a BIT key column and a BIT value column
    (bcp text '1'/'0'); the lake parquet carries a real boolean dtype."""
    cfg, tmp = env
    (tmp / "snap" / "rpt.tbit.csv").write_text(
        "k,flag,val\n"
        "a,1,1\n"
        "b,0,0\n")
    cfg["reports"]["tbit"] = {"table": "rpt.tbit", "keys": ["k", "flag"]}
    return cfg, tmp


def _write_lake_bit(tmp_path, rows):
    d = tmp_path / "ns1" / "curated" / "tbit"
    d.mkdir(parents=True)
    pd.DataFrame(rows, columns=["k", "flag", "val"]).to_parquet(d / "part-0.parquet")


def test_bit_columns_pass_with_boolean_dtype(env):
    cfg, tmp = _bit_env(env)
    _write_lake_bit(tmp, [("a", True, True), ("b", False, False)])
    assert reconcile.reconcile("tbit", "ns1", cfg)


def test_bit_columns_fail_on_flipped_value(env):
    cfg, tmp = _bit_env(env)
    _write_lake_bit(tmp, [("a", True, False), ("b", False, False)])
    assert not reconcile.reconcile("tbit", "ns1", cfg)


@pytest.mark.parametrize("v,expected", [
    (True, "1"), (False, "0"), (np.bool_(True), "1"), (np.bool_(False), "0"),
    (None, ""), (float("nan"), ""), (pd.NA, ""),
])
def test_norm_bit_and_null_cells(v, expected):
    assert reconcile._norm(v) == expected
