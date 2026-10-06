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
    snap = ROOT / "legacy_snapshots"
    snap.mkdir(exist_ok=True)
    (snap / "rpt.t.csv").write_text(
        "k1,k2,val,label\n"
        "a,1,1.50,foo\n"
        "b,2,2.25,bar\n")
    cfg = {"reports": {"t": {"table": "rpt.t", "keys": ["k1", "k2"]}},
           "numeric_tolerance": 1e-6, "sample_diffs": 10}
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
