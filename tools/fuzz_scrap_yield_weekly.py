#!/usr/bin/env python3
"""Fuzz + parity harness for mfg_lake.jobs.scrap_yield_weekly.

    python tools/fuzz_scrap_yield_weekly.py [--n 25] [--seed 20251117]

(a) generates N randomized variants of mes.production_count +
    mes.scrap_event + mes.production_order (dims copied) across four
    rotating calendar windows (canonical fall-back, spring-forward, and two
    ISO-year boundaries), covering UTC-vs-local week edges, +/-2h clipping,
    order overlap ties, boundary-inclusive candidates, NULL/duplicate/
    unknown feeds, zero-good weeks and scrap-only weeks;
(b) runs the job on each variant in a throwaway namespace and checks
    invariants plus an independent pure-Python oracle of the legacy procs
    (tools/oracle_scrap_yield_weekly.py, a literal cursor port);
(c) for the canonical seed: reconcile, byte-compare against
    legacy_snapshots/rpt.scrap_yield_weekly.csv, rerun determinism, earlier
    cutoff (expected identical -- AsOfUtc is never applied), corrupted-value
    negative control, canonical alloc == cursor port, oracle invariants;
(d) writes everything to a structured JSON log (default
    out/validation/fuzz_scrap_yield_weekly.json) with a report_meta block
    for tools/validation_report.py. Exit 1 on any failure.
"""
import argparse
import csv
import hashlib
import json
import os
import platform
import random
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "lakehouse" / "src"))
sys.path.insert(0, str(ROOT / "tools"))
from mfg_lake.common.paths import LAKE_ROOT, curated_dir  # noqa: E402
from fuzz_daily_production import Dims, _local_to_utc  # noqa: E402
import oracle_scrap_yield_weekly as oracle  # noqa: E402

RAW = ROOT / "data" / "raw"
SNAPSHOT = ROOT / "legacy_snapshots" / "rpt.scrap_yield_weekly.csv"
REPORT = "scrap_yield_weekly"
CANON_AS_OF = "2025-11-17 00:00:00"
KEYS = ["plant_id", "line_id", "iso_year", "iso_week"]
COLUMNS = KEYS + ["good_units", "scrap_units", "scrap_pct"]
SCHEMA = {"plant_id": "string", "line_id": "string", "iso_year": "int32",
          "iso_week": "int32", "good_units": "int32", "scrap_units": "int32",
          "scrap_pct": "decimal128(9, 2)"}
# dim.calendar is (re)generated for non-canonical windows; other dims copied
DIMS = ["dim.plant", "dim.line", "dim.sku", "dim.shift_pattern"]
TS = "%Y-%m-%d %H:%M:%S"
WINDOWS = [("canonical_fall_back", None),
           ("spring_forward", (date(2025, 3, 3), 35)),
           ("new_year_2026w01", (date(2025, 12, 15), 28)),
           ("new_year_2026w53", (date(2026, 12, 14), 28))]
SCRAP_TYPES = ["contamination", "splice", "edge trim", "startup waste"]


def _read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


# -------------------------------------------------------------- variants
def gen_variant(rng: random.Random, d: Dims, idx: int):
    """Returns (counts, scraps, orders, cal_rows or None, feats, window_name)."""
    win_name, win = WINDOWS[idx % len(WINDOWS)]
    cal_rows = None
    cal = d.calendar
    if win:
        start, n = win
        cal = [start + timedelta(days=i) for i in range(n)]
        cal_rows = [(c.isoformat(), c.isocalendar()[1], c.isoweekday(),
                     c.strftime("%A")) for c in cal]
    lines = rng.sample(sorted(d.lines), rng.randint(4, 10))
    skus = sorted(d.pack)
    feats = defaultdict(int)
    counts, scraps, orders = [], [], []
    scrap_id, order_id = itertools_counter(), itertools_counter()

    def add_count(line, sku, start_utc, minutes=15, total=None, good=None,
                  tag="random"):
        if total is None:
            total = rng.choice([0] * 3 + [rng.randint(0, 5000) for _ in range(30)])
        if good is None:
            good = 0 if total == 0 else rng.randint(max(0, int(total * 0.9)), total)
        counts.append({"line_id": line, "sku_id": sku,
                       "bucket_start_utc": start_utc.strftime(TS),
                       "bucket_end_utc": (start_utc + timedelta(minutes=minutes)).strftime(TS),
                       "total_units": str(total), "good_units": str(good)})
        feats[tag] += 1

    def add_scrap(line, ts, qty=None, tag="random", sid=None):
        if qty is None:
            qty = rng.choice([rng.randint(1, 400) for _ in range(9)] + [0])
            if qty == 0:
                tag = "scrap_qty_zero"
        scraps.append({"scrap_id": str(next(scrap_id) if sid is None else sid),
                       "line_id": line, "scrap_ts_utc": ts.strftime(TS),
                       "qty_units": str(qty), "scrap_type": rng.choice(SCRAP_TYPES)})
        feats[tag] += 1
        return ts

    def add_order(line, s, e, tag="order", oid=None):
        plant = d.lines.get(line, "PLT01")
        orders.append({"order_id": oid or f"O{next(order_id):08d}",
                       "line_id": line, "plant_id": plant,
                       "sku_id": rng.choice(skus), "sched_start_utc": s.strftime(TS),
                       "sched_end_utc": e.strftime(TS), "planned_qty": "1000"})
        feats[tag] += 1

    for line in lines:
        plant = d.lines[line]
        iana = d.tz[plant]
        sku = rng.choice(skus)
        days = rng.sample(cal, min(len(cal), rng.randint(3, 8)))
        for day in days:
            base = datetime(day.year, day.month, day.day)
            # regular aligned buckets over the production day (subsample)
            for b in rng.sample(range(96), rng.randint(5, 20)):
                add_count(line, sku, _local_to_utc(base + timedelta(hours=6, minutes=15 * b),
                                                   iana), 15)
            # straddling the Monday 06:00-local boundary (production_day rule)
            if day.isoweekday() == 1:
                a = _local_to_utc(base + timedelta(hours=6), iana)
                add_count(line, sku, a - timedelta(minutes=rng.randint(1, 14)), 15,
                          tag="straddle_0600_monday")
                add_count(line, sku, a, 15, tag="at_0600")
        # a whole zero-good week for this line (pct NULL when no scrap)
        zday = rng.choice(cal)
        zmon = zday - timedelta(days=zday.isoweekday() - 1)
        for b in range(0, 96, 12):
            add_count(line, sku,
                      _local_to_utc(datetime(zmon.year, zmon.month, zmon.day)
                                    + timedelta(hours=6, minutes=15 * b), iana),
                      15, total=0, good=0, tag="zero_good_week")
        # NULLs, duplicates, unknown sku/line, out-of-window
        for _ in range(rng.randint(0, 3)):
            r = dict(rng.choice(counts))
            r[rng.choice(["total_units", "good_units", "sku_id",
                          "bucket_start_utc", "line_id"])] = ""
            counts.append(r)
            feats["null_field"] += 1
        for _ in range(rng.randint(1, 4)):
            counts.append(dict(rng.choice(counts)))
            feats["duplicate_bucket"] += 1
        for _ in range(rng.randint(1, 3)):
            r = dict(rng.choice(counts))
            r["sku_id"] = f"SKU-ZZ{rng.randint(10, 99)}"
            counts.append(r)
            feats["unknown_sku"] += 1
        first = _local_to_utc(datetime.combine(cal[0], datetime.min.time())
                              + timedelta(hours=6), iana)
        last = _local_to_utc(datetime.combine(cal[-1], datetime.min.time())
                             + timedelta(days=1, hours=6), iana)
        add_count(line, sku, first - timedelta(minutes=rng.randint(1, 600)), 15,
                  tag="before_window")
        add_count(line, sku, last + timedelta(minutes=rng.randint(0, 600)), 15,
                  tag="after_window")

        # ------- orders + scrap events (allocation edge cases)
        for day in days:
            base = datetime(day.year, day.month, day.day)
            ts = _local_to_utc(base + timedelta(hours=rng.randint(7, 20)), iana)
            # 2-3 way overlap around the event
            add_order(line, ts - timedelta(hours=1), ts + timedelta(hours=1))
            add_order(line, ts - timedelta(minutes=30), ts + timedelta(minutes=30),
                      tag="order_overlap")
            if rng.random() < 0.5:
                add_order(line, ts - timedelta(minutes=45), ts + timedelta(hours=3),
                          tag="order_overlap")
            add_scrap(line, ts, tag="scrap_in_overlap")
            add_scrap(line, ts + timedelta(minutes=7), qty=0, tag="scrap_qty_zero")
            # equal-overlap tie: string-comparable ids 'ORD-10' < 'ORD-9'
            t2 = ts + timedelta(hours=4)
            add_order(line, t2 - timedelta(hours=1), t2 + timedelta(hours=1),
                      tag="order_tie", oid=f"ORD-9-{day.month:02d}{day.day:02d}")
            add_order(line, t2 - timedelta(hours=1), t2 + timedelta(hours=1),
                      tag="order_tie", oid=f"ORD-10-{day.month:02d}{day.day:02d}")
            add_scrap(line, t2, tag="scrap_order_tie")
            # boundary-inclusive candidates and end==ts-1s exclusion
            t3 = ts + timedelta(hours=8)
            add_order(line, t3, t3 + timedelta(hours=2), tag="order_start_eq_ts")
            add_order(line, t3 - timedelta(hours=2), t3, tag="order_end_eq_ts")
            add_order(line, t3 - timedelta(hours=2), t3 - timedelta(seconds=1),
                      tag="order_end_ts_minus_1s")
            add_scrap(line, t3, tag="scrap_boundary")
            # long order clipped at the +-2h window
            t4 = ts + timedelta(hours=12)
            add_order(line, t4 - timedelta(hours=10), t4 + timedelta(hours=10),
                      tag="order_long_clipped")
            add_scrap(line, t4, tag="scrap_long_clipped")
            # other-line order overlapping the event is ignored
            other = rng.choice([l for l in d.lines if l != line])
            add_order(other, ts - timedelta(hours=1), ts + timedelta(hours=1),
                      tag="order_other_line")
            # event with no candidate order -> UNALLOCATED
            add_scrap(line, ts + timedelta(days=3), tag="scrap_no_order")
        # scrap at the Sun 23:59:59 / Mon 00:00:00 UTC boundary (UTC-vs-local week)
        for day in cal:
            if day.isoweekday() == 7:
                sun = datetime(day.year, day.month, day.day)
                add_scrap(line, sun + timedelta(hours=23, minutes=59, seconds=59),
                          tag="scrap_sun_235959_utc")
                add_scrap(line, sun + timedelta(days=1), tag="scrap_mon_000000_utc")
                break
        # scrap around Jan 1 (ISO year boundary) when the window covers it
        for day in cal:
            if day.month == 1 and day.day <= 3 or (day.month == 12 and day.day >= 29):
                add_scrap(line, datetime(day.year, day.month, day.day, 12),
                          tag="scrap_around_jan1")
                break
        # scrap in a week with no production for this line -> dropped by LEFT JOIN
        add_scrap(line, last + timedelta(days=9), tag="scrap_outside_week")
        # scrap on an unknown line, NULL fields, duplicate event (new scrap_id)
        add_scrap("PLT99-L9", ts, tag="scrap_unknown_line")
        for _ in range(rng.randint(0, 3)):
            r = dict(rng.choice(scraps))
            r[rng.choice(["line_id", "scrap_ts_utc", "qty_units", "scrap_type"])] = ""
            r["scrap_id"] = str(next(scrap_id))
            scraps.append(r)
            feats["null_field"] += 1
        if scraps:
            r = dict(rng.choice(scraps))
            if r["scrap_id"]:
                r["scrap_id"] = str(next(scrap_id))
                scraps.append(r)
                feats["duplicate_scrap_event"] += 1
    counts.append(dict(counts[0], line_id="PLT99-L9"))
    feats["unknown_line"] += 1
    rng.shuffle(counts)
    rng.shuffle(scraps)
    rng.shuffle(orders)
    return counts, scraps, orders, cal_rows, dict(feats), win_name


def itertools_counter():
    i = 700000
    while True:
        yield i
        i += 1


# ------------------------------------------------------------ invariants
def _pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return Decimal(str(v))


def check_report(df, schema, d, oracle_rep, window, scraps, counts):
    res = {}

    def put(name, bad, detail_ok="ok"):
        res[name] = {"pass": not bad, "detail": detail_ok if not bad else "; ".join(bad[:5])
                     + (f" (+{len(bad) - 5} more)" if len(bad) > 5 else "")}

    got_schema = {f.name: str(f.type) for f in schema}
    put("schema_contract", [f"{c}: {got_schema.get(c)} != {t}" for c, t in SCHEMA.items()
                            if got_schema.get(c) != t]
        + ([f"columns {list(got_schema)}"] if list(got_schema) != COLUMNS else []))
    recs = df.to_dict("records")
    put("unique_keys", [] if not df.duplicated(KEYS).any() else ["duplicate report keys"])
    got = {(r["plant_id"], r["line_id"], int(r["iso_year"]), int(r["iso_week"])):
           (int(r["good_units"]), int(r["scrap_units"]), _pct(r["scrap_pct"]))
           for r in recs}
    put("oracle_keys", [f"missing {k}" for k in sorted(set(oracle_rep) - set(got))]
        + [f"unexpected {k}" for k in sorted(set(got) - set(oracle_rep))])
    put("oracle_values",
        [f"{k}: job {got[k]} oracle {oracle_rep[k]}" for k in sorted(set(got) & set(oracle_rep))
         if (got[k][0], got[k][1],
             None if got[k][2] is None else Decimal(got[k][2])) != oracle_rep[k]])
    put("scrap_pct_rule",
        [f"{k}: pct {got[k][2]} (good {got[k][0]}, scrap {got[k][1]})"
         for k in got
         if (got[k][0] + got[k][1] == 0) != (got[k][2] is None)
         or (got[k][0] + got[k][1] != 0
             and Decimal(got[k][2]) != oracle.scrap_pct(got[k][0], got[k][1]))])
    lo, hi = window
    bad_iso = []
    for r in recs:
        try:
            mon = date.fromisocalendar(int(r["iso_year"]), int(r["iso_week"]), 1)
        except ValueError:
            bad_iso.append(f"invalid ISO {r['iso_year']}-W{r['iso_week']}")
            continue
        if not (lo - timedelta(days=7) <= mon <= hi + timedelta(days=7)):
            bad_iso.append(f"{r['line_id']} ISO {r['iso_year']}-W{r['iso_week']} "
                           f"outside window {lo}..{hi}")
    put("iso_calendar_valid", bad_iso)
    put("no_unknown_lines",
        [f"line {r['line_id']}" for r in recs if r["line_id"].strip() not in d.lines]
        + [f"{r['line_id']}: plant {r['plant_id']} != dim.line "
           f"{d.lines[r['line_id'].strip()]}" for r in recs
           if r["line_id"].strip() in d.lines
           and r["plant_id"] != d.lines[r["line_id"].strip()].strip()])
    valid_scrap = sum(int(r["qty_units"]) for r in scraps
                      if not oracle._null(r, oracle.SE_NOT_NULL)
                      and r["line_id"].strip() in d.lines)
    put("scrap_units_le_input",
        [f"output scrap {sum(r['scrap_units'] for r in recs)} > input {valid_scrap}"]
        if sum(int(r["scrap_units"]) for r in recs) > valid_scrap else [])
    n_null = sum(1 for r in counts
                 if any(r.get(c) in ("", None) for c in oracle.PC_NOT_NULL))
    res["_inputs"] = {"rows": len(counts), "null_rows": n_null,
                      "scrap_rows_in": len(scraps), "output_rows": len(recs)}
    return res


def check_alloc(alloc_rows, scraps, orders):
    """alloc_rows: list of (scrap_id, order_id, qty) from the job."""
    res = {}
    got = Counter(alloc_rows)
    exp = Counter(oracle.cursor_alloc(scraps, orders))
    bad = [f"missing {k} x{n}" for k, n in (exp - got).items()]
    bad += [f"extra {k} x{n}" for k, n in (got - exp).items()]
    res["alloc_matches_cursor_port"] = {
        "pass": not bad, "detail": "; ".join(bad[:5]) or "ok"}
    by_scrap = Counter()
    for sid, _, q in alloc_rows:
        by_scrap[sid] += q
    valid = {int(r["scrap_id"]): int(r["qty_units"]) for r in scraps
             if not oracle._null(r, oracle.SE_NOT_NULL)}
    bad = [f"scrap {sid}: alloc {by_scrap.get(sid, 0)} != qty {q}"
           for sid, q in valid.items() if by_scrap.get(sid, 0) != q]
    res["alloc_conserves_qty"] = {"pass": not bad, "detail": "; ".join(bad[:5]) or "ok"}
    return res


def load_lake(ns):
    d = curated_dir(REPORT, ns)
    tbl = pq.read_table(sorted(d.glob("*.parquet")))
    df = tbl.to_pandas()
    df["scrap_pct"] = df["scrap_pct"].astype(object).where(df["scrap_pct"].notna(), None)
    return df, tbl.schema


def _job_alloc(job, spark, raw_dir):
    from mfg_lake.common.io import read_raw
    return [(r.scrap_id, r.order_id, r.qty_units)
            for r in job.allocate_scrap_to_orders(
                read_raw(spark, "mes.scrap_event"),
                read_raw(spark, "mes.production_order")).collect()]


# ------------------------------------------------------------- canonical
def to_bcp_csv(df: pd.DataFrame) -> bytes:
    """Render like the MANIFEST bcp export (header, NULL = empty, ORDER BY key).
    The committed extract uses CRLF row terminators."""
    out = [",".join(COLUMNS)]
    for r in df.sort_values(KEYS).to_dict("records"):
        p = _pct(r["scrap_pct"])
        out.append(",".join([r["plant_id"], r["line_id"], str(r["iso_year"]),
                             str(r["iso_week"]), str(r["good_units"]),
                             str(r["scrap_units"]), "" if p is None else f"{p:.2f}"]))
    return ("\r\n".join(out) + "\r\n").encode()


def _sha(b):
    return hashlib.sha256(b).hexdigest()


def run_reconcile(ns):
    p = subprocess.run([sys.executable, str(ROOT / "tools" / "reconcile.py"),
                        "--report", REPORT, "--ns", ns], capture_output=True, text=True,
                       env={**os.environ, "LAKE_ROOT": str(LAKE_ROOT)})
    return p.returncode, p.stdout


def _frames_equal(a, b):
    a = a.sort_values(KEYS).reset_index(drop=True)
    b = b.sort_values(KEYS).reset_index(drop=True)
    return bool(a.equals(b) and list(a.dtypes) == list(b.dtypes))


def canonical_checks(job, spark, tag, log):
    os.environ.pop("RAW_DIR", None)
    out = {}
    ns = f"fuzz-{tag}-canonical"
    job.run(ns, CANON_AS_OF, spark)
    df, schema = load_lake(ns)
    rc, txt = run_reconcile(ns)
    out["reconcile"] = {"pass": rc == 0, "exit_code": rc, "output": txt}
    snap = SNAPSHOT.read_bytes()
    mine = to_bcp_csv(df)
    snap_rows = snap.decode().splitlines()
    mine_rows = mine.decode().splitlines()
    snap_keys = {",".join(l.split(",")[:4]) for l in snap_rows[1:]}
    mine_keys = {",".join(l.split(",")[:4]) for l in mine_rows[1:]}
    out["byte_compare"] = {"pass": mine == snap, "snapshot_sha256": _sha(snap),
                           "lake_csv_sha256": _sha(mine), "snapshot_bytes": len(snap),
                           "lake_bytes": len(mine), "snapshot_rows": len(snap_rows) - 1,
                           "lake_rows": len(mine_rows) - 1,
                           "matched_keys": len(snap_keys & mine_keys),
                           "missing_keys": len(snap_keys - mine_keys),
                           "extra_keys": len(mine_keys - snap_keys),
                           "mismatched_rows": len(set(snap_rows[1:]) - set(mine_rows[1:])),
                           "line_terminator": "CRLF" if b"\r\n" in snap else "LF"}
    d = Dims(RAW)
    scraps, orders = _read_csv(RAW / "mes.scrap_event.csv"), \
        _read_csv(RAW / "mes.production_order.csv")
    counts = _read_csv(RAW / "mes.production_count.csv")
    oracle_rep = oracle.report(d, counts, scraps)
    out["schema"] = check_report(df, schema, d, oracle_rep,
                                 (min(d.calendar), max(d.calendar)), scraps, counts)
    out["schema"].update(check_alloc(_job_alloc(job, spark, RAW), scraps, orders))
    log(f"  canonical reconcile={'PASS' if rc == 0 else 'FAIL'} "
        f"byte_compare={'PASS' if mine == snap else 'FAIL'} sha256={_sha(mine)[:16]}")
    # rerun into the same namespace -> identical dataframe + dtypes
    job.run(ns, CANON_AS_OF, spark)
    df2, _ = load_lake(ns)
    out["rerun_deterministic"] = {"pass": _frames_equal(df, df2) and to_bcp_csv(df2) == mine}
    log(f"  rerun deterministic={out['rerun_deterministic']['pass']}")
    # earlier cutoff: legacy ignores AsOfUtc -> identical output, same exit code
    early = "2025-11-03 00:00:00"
    ns_e = f"fuzz-{tag}-early"
    job.run(ns_e, early, spark)
    df3, _ = load_lake(ns_e)
    rc3, _ = run_reconcile(ns_e)
    out["earlier_cutoff"] = {"as_of_utc": early, "identical_to_canonical": _frames_equal(df, df3),
                             "reconcile_exit_code": rc3,
                             "pass": _frames_equal(df, df3) and rc3 == rc,
                             "note": "as-of-utc is accepted but not applied (legacy parity); "
                                     "identical output expected. KAN-9 AC4 (earlier cutoff "
                                     "fails) not met by design"}
    log(f"  earlier cutoff {early}: identical={out['earlier_cutoff']['identical_to_canonical']} "
        f"reconcile_exit={rc3}")
    # negative control: one corrupted value must fail reconcile
    ns_c = f"fuzz-{tag}-corrupt"
    src, dst = curated_dir(REPORT, ns), curated_dir(REPORT, ns_c)
    shutil.rmtree(dst, ignore_errors=True)
    dst.mkdir(parents=True)
    tbl = pq.read_table(sorted(src.glob("*.parquet")))
    import pyarrow as pa
    su = tbl.column("scrap_units").to_pylist()
    su[0] += 1
    tbl = tbl.set_column(tbl.schema.get_field_index("scrap_units"), "scrap_units",
                         pa.array(su, type=pa.int32()))
    pq.write_table(tbl, dst / "part-00000.parquet")
    rc4, txt4 = run_reconcile(ns_c)
    out["corrupted_value"] = {"pass": rc4 != 0, "reconcile_exit_code": rc4,
                              "mutation": "row 0 scrap_units += 1",
                              "output_tail": "\n".join(txt4.strip().splitlines()[-3:])}
    log(f"  corrupted value -> reconcile exit {rc4} ({'expected FAIL' if rc4 else 'UNEXPECTED PASS'})")
    return out, df


def checker_self_test(df, schema, d, oracle_rep, window, scraps, orders, alloc, counts):
    """Mutate a passing output; each mutant must trip at least one invariant."""
    def rep(df_mut):
        r = check_report(df_mut, schema, d, oracle_rep, window, scraps, counts)
        return [k for k, v in r.items() if not k.startswith("_") and not v["pass"]]

    def al(a_mut):
        r = check_alloc(a_mut, scraps, orders)
        return [k for k, v in r.items() if not v["pass"]]

    muts = {}
    m = df.copy()
    m.loc[0, ["good_units", "scrap_units"]] = [799, 1]
    m["scrap_pct"] = [Decimal("0.12") if i == 0 else p for i, p in enumerate(m["scrap_pct"])]
    muts["bankers_rounding"] = rep(m)
    m = df.copy()
    m.loc[0, ["good_units", "scrap_units"]] = [0, 0]
    m["scrap_pct"] = [Decimal("0.00") if i == 0 else p for i, p in enumerate(m["scrap_pct"])]
    muts["pct_not_null_on_zero"] = rep(m)
    muts["local_week_shift"] = rep(df.assign(
        iso_week=[w + 1 if i == 0 else w for i, w in enumerate(df["iso_week"])]))
    muts["iso_year_off"] = rep(df.assign(
        iso_year=[y + 1 if i == 0 else y for i, y in enumerate(df["iso_year"])]))
    muts["unknown_line_leak"] = rep(pd.concat(
        [df, df.head(1).assign(line_id="PLT99-L9")], ignore_index=True))
    muts["duplicate_key"] = rep(pd.concat([df, df.head(1)], ignore_index=True))
    # alloc mutants
    a = list(alloc)
    by_scrap = defaultdict(list)
    for i, r in enumerate(a):
        by_scrap[r[0]].append(i)
    multi = next((idxs for idxs in by_scrap.values() if len(idxs) > 1), None)
    if multi:
        i1, i2 = multi[0], multi[1]
        a[i1] = (a[i1][0], a[i1][1], a[i1][2] - 1)
        a[i2] = (a[i2][0], a[i2][1], a[i2][2] + 1)
    muts["alloc_remainder_to_smallest"] = al(a)
    a2 = list(alloc)
    a2.pop(0)
    muts["alloc_drop_row"] = al(a2)
    return {name: {"caught": bool(t), "tripped": t} for name, t in muts.items()}


# ------------------------------------------------------------------ main
def env_info(spark, args):
    git = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=ROOT)
    import pyspark
    jvm = spark.sparkContext._jvm
    return {"python": platform.python_version(), "pyspark": pyspark.__version__,
            "spark": spark.version, "java": jvm.System.getProperty("java.version"),
            "jvm_tzdb": str(jvm.java.time.zone.ZoneRulesProvider.getVersions("UTC").lastKey()),
            "pandas": pd.__version__, "platform": platform.platform(),
            "spark_session_tz": spark.conf.get("spark.sql.session.timeZone"),
            "as_of_utc": CANON_AS_OF, "fuzz_seed": args.seed, "variants": args.n,
            "seed_py_SEED": 20251020, "git_commit": git.stdout.strip(),
            "started_utc": datetime.now(timezone.utc).strftime(TS)}


def report_meta():
    return {"ticket": "KAN-9", "pipeline": "PL_Scrap_Yield",
            "table": "rpt.scrap_yield_weekly",
            "earlier_cutoff_expect": "identical",
            "earlier_cutoff_note": "KAN-9 AC4 expects a FAIL here; not met by design "
                                   "(user-approved): the legacy procs never receive AsOfUtc, "
                                   "so identical output is the parity result - see deviations",
            "parity_notes": [
                "<b>Earlier cutoff (KAN-9 AC4).</b> <code>--as-of-utc</code> is accepted, "
                "format-validated and not applied, as in the legacy pipeline (the procs "
                "receive no AsOfUtc). An earlier cutoff therefore yields identical output "
                "and the same reconcile result; AC4's \"earlier cutoff fails\" is not met "
                "by design (user-approved).",
                "<b>UTC vs local ISO week.</b> Scrap is bucketed by the ISO week of the "
                "<b>UTC</b> <code>scrap_ts_utc</code>; good units by the ISO week of the "
                "local <code>production_day</code>. A local-week port mismatches 33/96 rows.",
                "<b>No <code>dim.sku</code> join.</b> <code>stg.production_local</code> is "
                "summed as-is, so unknown-SKU buckets count toward <code>good_units</code>. "
                "The two <code>SKU-XX99</code> seed rows now carry 0 units (user-approved "
                "seed fix) so the snapshot stays unchanged.",
                "<b><code>stg.scrap_alloc</code> is not persisted.</b> The rpt proc reads "
                "<code>mes.scrap_event</code> directly; the allocation is recomputed in the "
                "job for parity/logging only and is checked against a literal cursor port "
                "(<code>alloc_matches_cursor_port</code>).",
                "<b>Alloc semantics.</b> <code>ts BETWEEN start AND end</code> is "
                "inclusive; overlap is clipped to the &plusmn;7200s window; the rounding "
                "remainder goes to the largest overlap, ties broken by the "
                "<code>order_id</code> string; no candidate &rarr; <code>UNALLOCATED</code>.",
                "<b>NULL inputs</b> (impossible in legacy because the columns are NOT NULL) "
                "are dropped and counted, not failed. <b>INT overflow</b> raises, like SQL "
                "Server's arithmetic overflow.",
                "The ADF JSON in <code>lakehouse/adf/</code> was reviewed, not deployed. "
                "Nothing here ran on ADF or Databricks.",
                "<b>&plusmn;7200 in the alloc proc.</b> As written, <code>@ts - 7200</code> / "
                "<code>@ts + 7200</code> on a <code>DATETIME2(0)</code> is an operand type "
                "clash in SQL Server (the proc would error). It is read as &plusmn;7200 "
                "<b>seconds</b>, matching the <code>DATEADD(SECOND, &plusmn;7200, @ts)</code> "
                "bounds in the same WHERE clause. Approved deviation. It affects only the "
                "allocation, which does not feed the report.",
                "<b>Line terminator.</b> MANIFEST shows <code>bcp -r\\n</code>, but the "
                "committed extract uses CRLF. The byte-compare renders CRLF.",
                "<b>Coverage gaps.</b> <code>stg.scrap_alloc</code> has no prod extract, so it "
                "is checked against the literal cursor port only, not prod. The "
                "<code>order_id</code> tie-break uses binary string order, while legacy uses "
                "the server collation; they agree for the upper-case/numeric ids in the data.",
                "<b>Local path.</b> AC1 gives the local path as "
                "<code>out/&lt;ns&gt;/curated/scrap_yield_weekly</code>. The shared "
                "<code>common/paths.curated_dir</code> (PR #5) maps the abfss URI to "
                "<code>out/&lt;ns&gt;/curated/manufacturing/scrap_yield_weekly</code>, and the "
                "job uses the shared mapping. The abfss target matches AC1 exactly."]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=25)
    ap.add_argument("--seed", type=int, default=20251117)
    ap.add_argument("--log", default=str(ROOT / "out" / "validation" / "fuzz_scrap_yield_weekly.json"))
    ap.add_argument("--keep", action="store_true", help="keep throwaway namespaces")
    args = ap.parse_args()

    from mfg_lake.common.spark import get_spark
    from mfg_lake.jobs import scrap_yield_weekly as job

    def log(msg):
        print(msg, flush=True)

    spark = get_spark("fuzz_scrap_yield_weekly")
    spark.sparkContext.setLogLevel("ERROR")
    tag = f"{args.seed}"
    result = {"tool": "tools/fuzz_scrap_yield_weekly.py",
              "report_meta": report_meta(),
              "environment": env_info(spark, args)}
    t0 = time.time()

    log(f"== canonical seed (AS_OF_UTC='{CANON_AS_OF}') ==")
    result["canonical"], canon_df = canonical_checks(job, spark, tag, log)

    variants = []
    work = LAKE_ROOT / f"fuzz-{tag}-raw"
    d0 = Dims(RAW)
    for i in range(args.n):
        vt = time.time()
        rng = random.Random(args.seed * 1000 + i)
        vdir = work / f"v{i:03d}"
        shutil.rmtree(vdir, ignore_errors=True)
        vdir.mkdir(parents=True)
        for t in DIMS:
            shutil.copy(RAW / f"{t}.csv", vdir / f"{t}.csv")
        counts, scraps, orders, cal_rows, feats, window = gen_variant(rng, d0, i)
        if cal_rows:
            _write_csv(vdir / "dim.calendar.csv",
                       ["calendar_date", "iso_week", "day_of_week", "day_name"], cal_rows)
        else:
            shutil.copy(RAW / "dim.calendar.csv", vdir / "dim.calendar.csv")
        _write_csv(vdir / "mes.production_count.csv",
                   ["bucket_id", "line_id", "sku_id", "bucket_start_utc", "bucket_end_utc",
                    "total_units", "good_units"],
                   [(n + 1, r["line_id"], r["sku_id"], r["bucket_start_utc"],
                     r["bucket_end_utc"], r["total_units"], r["good_units"])
                    for n, r in enumerate(counts)])
        _write_csv(vdir / "mes.scrap_event.csv",
                   ["scrap_id", "line_id", "scrap_ts_utc", "qty_units", "scrap_type"],
                   [(r["scrap_id"], r["line_id"], r["scrap_ts_utc"], r["qty_units"],
                     r["scrap_type"]) for r in scraps])
        _write_csv(vdir / "mes.production_order.csv",
                   ["order_id", "line_id", "plant_id", "sku_id", "sched_start_utc",
                    "sched_end_utc", "planned_qty"],
                   [(r["order_id"], r["line_id"], r["plant_id"], r["sku_id"],
                     r["sched_start_utc"], r["sched_end_utc"], r["planned_qty"])
                    for r in orders])
        d = Dims(vdir)
        cal = [date.fromisoformat(r["calendar_date"])
               for r in _read_csv(vdir / "dim.calendar.csv")]
        ns = f"fuzz-{tag}-v{i:03d}"
        os.environ["RAW_DIR"] = str(vdir)
        as_of = (datetime(2025, 11, 17) - timedelta(hours=rng.randint(0, 24 * 30))).strftime(TS)
        entry = {"variant": i, "rng_seed": args.seed * 1000 + i, "namespace": ns,
                 "calendar_window": window, "as_of_utc": as_of, "features": feats}
        try:
            job.run(ns, as_of, spark)
            df, schema = load_lake(ns)
            oracle_rep = oracle.report(d, counts, scraps)
            inv = check_report(df, schema, d, oracle_rep, (min(cal), max(cal)),
                               scraps, counts)
            inv.update(check_alloc(_job_alloc(job, spark, vdir), scraps, orders))
            entry["inputs"] = inv.pop("_inputs")
            entry["invariants"] = inv
            if i == 0:
                result["checker_self_test"] = checker_self_test(
                    df, schema, d, oracle_rep, (min(cal), max(cal)), scraps, orders,
                    _job_alloc(job, spark, vdir), counts)
        except Exception as e:  # job crash is a failed variant, not a harness crash
            entry["invariants"] = {"job_ran": {"pass": False, "detail": f"{type(e).__name__}: {e}"}}
        entry["pass"] = all(v["pass"] for v in entry["invariants"].values())
        entry["violations"] = [k for k, v in entry["invariants"].items() if not v["pass"]]
        entry["seconds"] = round(time.time() - vt, 2)
        variants.append(entry)
        log(f"  variant {i:03d} [{window:<19}] out={entry.get('inputs', {}).get('output_rows', '?'):>4} "
            f"{'PASS' if entry['pass'] else 'FAIL ' + ','.join(entry['violations'])}")
        if not args.keep:
            shutil.rmtree(LAKE_ROOT / ns, ignore_errors=True)
    os.environ.pop("RAW_DIR", None)
    if not args.keep:
        shutil.rmtree(work, ignore_errors=True)
    result["variants"] = variants

    c = result["canonical"]
    st = result.get("checker_self_test", {})
    result["summary"] = {
        "variants_total": len(variants),
        "variants_passed": sum(v["pass"] for v in variants),
        "canonical_reconcile": c["reconcile"]["pass"],
        "canonical_byte_compare": c["byte_compare"]["pass"],
        "rerun_deterministic": c["rerun_deterministic"]["pass"],
        "earlier_cutoff_identical": c["earlier_cutoff"]["pass"],
        "corrupted_value_detected": c["corrupted_value"]["pass"],
        "checker_mutants_caught": f"{sum(m['caught'] for m in st.values())}/{len(st)}",
        "seconds": round(time.time() - t0, 1),
    }
    ok = (all(v["pass"] for v in variants) and c["reconcile"]["pass"]
          and c["byte_compare"]["pass"] and c["rerun_deterministic"]["pass"]
          and c["earlier_cutoff"]["pass"] and c["corrupted_value"]["pass"]
          and all(m["caught"] for m in st.values()))
    result["summary"]["overall_pass"] = ok
    Path(args.log).parent.mkdir(parents=True, exist_ok=True)
    Path(args.log).write_text(json.dumps(result, indent=2, default=str))
    log("== summary ==")
    for k, v in result["summary"].items():
        log(f"  {k:<26} {v}")
    log(f"log -> {args.log}")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
