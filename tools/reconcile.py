#!/usr/bin/env python3
"""Reconcile a lakehouse report against its legacy rpt snapshot.

    python tools/reconcile.py --report <name> --ns <NS>

Compares out/<NS>/curated/<report>/*.parquet with
legacy_snapshots/<table>.csv using the contract in
tools/reconcile_config.yaml:

  * row count
  * key-set diff (missing / extra keys)
  * per-column value checks: numeric checksum + mismatch counts
    (abs tolerance 1e-6); up to 10 sample diffs shown
  * prints a PASS/FAIL table, writes out/<NS>/reconciliation/<report>.md
  * exit code 1 on any FAIL
"""
import argparse
import math
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "lakehouse" / "src"))
from mfg_lake.common.paths import curated_dir, reconciliation_dir  # noqa: E402

TOL_DEFAULT = 1e-6


def load_config():
    return yaml.safe_load((ROOT / "tools" / "reconcile_config.yaml").read_text())


def load_snapshot(table: str) -> pd.DataFrame:
    p = ROOT / "legacy_snapshots" / f"{table}.csv"
    if not p.exists():
        raise FileNotFoundError(f"legacy snapshot missing: {p} (run `make legacy-run`)")
    return pd.read_csv(p, dtype=str, keep_default_na=False)


def load_lake(report: str, ns: str) -> pd.DataFrame:
    d = curated_dir(report, ns)
    files = sorted(d.glob("*.parquet")) if d.exists() else []
    if not files:
        raise FileNotFoundError(f"no curated parquet under {d} (run the job first)")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _norm(v):
    """Normalize a cell to a canonical string for equality checks."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d %H:%M:%S") if (v.hour or v.minute or v.second) \
            else v.strftime("%Y-%m-%d")
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    return str(v)


def _num(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _key(df: pd.DataFrame, keys):
    return df[keys].apply(lambda r: "|".join(_norm(x) for x in r), axis=1)


def reconcile(report: str, ns: str, cfg) -> bool:
    spec = cfg["reports"][report]
    keys = spec["keys"]
    tol = cfg.get("numeric_tolerance", TOL_DEFAULT)
    n_samples = cfg.get("sample_diffs", 10)

    legacy = load_snapshot(spec["table"])
    lake = load_lake(report, ns)

    # align column sets
    cols = [c for c in legacy.columns if c in lake.columns]
    missing_cols = [c for c in legacy.columns if c not in lake.columns]
    extra_cols = [c for c in lake.columns if c not in legacy.columns]
    legacy, lake = legacy[cols], lake[cols].copy()
    for c in cols:
        lake[c] = lake[c].map(_norm)

    controls = []
    diffs = []

    # control 1: row count
    ok = len(lake) == len(legacy)
    controls.append(("row_count", f"lake={len(lake)} legacy={len(legacy)}", ok))

    # control 2: key sets
    lk, gk = set(_key(lake, keys)), set(_key(legacy, keys))
    missing, extra = sorted(gk - lk), sorted(lk - gk)
    controls.append(("key_set", f"missing={len(missing)} extra={len(extra)}",
                     not missing and not extra))

    # per-column checks on joined key rows
    lmap = {_key(lake, keys).iloc[i]: i for i in range(len(lake))}
    gmap = {_key(legacy, keys).iloc[i]: i for i in range(len(legacy))}
    common = sorted(lk & gk)
    col_stats = {c: {"mismatches": 0, "checksum_legacy": 0.0,
                     "checksum_lake": 0.0, "numeric": True} for c in cols}
    shown = 0
    for k in common:
        lr, gr = lake.iloc[lmap[k]], legacy.iloc[gmap[k]]
        for c in cols:
            lv, gv = _norm(lr[c]), _norm(gr[c])
            ln, gn = _num(lv), _num(gv)
            st = col_stats[c]
            if ln is None or gn is None:
                st["numeric"] = False
                if lv != gv:
                    st["mismatches"] += 1
                    if shown < n_samples:
                        diffs.append(f"key={k} col={c}: lake='{lv}' legacy='{gv}'")
                        shown += 1
            else:
                st["checksum_lake"] += ln
                st["checksum_legacy"] += gn
                if abs(ln - gn) > tol:
                    st["mismatches"] += 1
                    if shown < n_samples:
                        diffs.append(f"key={k} col={c}: lake='{lv}' legacy='{gv}'")
                        shown += 1

    for c in cols:
        st = col_stats[c]
        if st["numeric"]:
            ck_ok = abs(st["checksum_lake"] - st["checksum_legacy"]) <= tol * max(1, len(common))
            controls.append((f"col[{c}].checksum",
                             f"lake={st['checksum_lake']:.4f} legacy={st['checksum_legacy']:.4f}",
                             ck_ok))
        controls.append((f"col[{c}].mismatches", f"n={st['mismatches']}",
                         st["mismatches"] == 0))

    if missing_cols:
        controls.append(("columns_missing_in_lake", ",".join(missing_cols), False))
    if extra_cols:
        controls.append(("extra_columns_in_lake", ",".join(extra_cols), False))

    # report
    lines = [f"# Reconciliation: {report} (ns={ns})", "",
             f"legacy: `{spec['table']}` vs lake: `out/{ns}/curated/{report}`", "",
             "| control | detail | result |", "|---|---|---|"]
    for name, detail, ok in controls:
        lines.append(f"| {name} | {detail} | {'PASS' if ok else 'FAIL'} |")
    if diffs:
        lines += ["", "## sample diffs", "", "```"] + diffs + ["```"]
    if missing:
        lines += ["", f"missing keys (in legacy, not lake): {missing[:20]}"]
    if extra:
        lines += ["", f"extra keys (in lake, not legacy): {extra[:20]}"]

    out_dir = reconciliation_dir(ns)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{report}.md").write_text("\n".join(lines))

    width = max(len(n) for n, _, _ in controls)
    for name, detail, ok in controls:
        print(f"{'PASS' if ok else 'FAIL'}  {name:<{width}}  {detail}")
    for dline in diffs:
        print("  diff:", dline)

    passed = all(ok for _, _, ok in controls)
    print(f"\n{'PASS' if passed else 'FAIL'}: {report} ns={ns} "
          f"({sum(1 for _,_,o in controls if o)}/{len(controls)} controls) -> "
          f"{out_dir / (report + '.md')}")
    return passed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report")
    ap.add_argument("--ns", default="dev")
    ap.add_argument("--list-jobs", action="store_true",
                    help="print reports with a job module in mfg_lake/jobs")
    args = ap.parse_args()
    cfg = load_config()

    if args.list_jobs:
        jobs = {p.stem for p in (ROOT / "lakehouse/src/mfg_lake/jobs").glob("*.py")}
        jobs.discard("__init__")
        for r in sorted(cfg["reports"]):
            if r in jobs:
                print(r)
        return

    if not args.report or args.report not in cfg["reports"]:
        print(f"unknown report {args.report!r}; registered: {sorted(cfg['reports'])}",
              file=sys.stderr)
        sys.exit(2)
    sys.exit(0 if reconcile(args.report, args.ns, cfg) else 1)


if __name__ == "__main__":
    main()
