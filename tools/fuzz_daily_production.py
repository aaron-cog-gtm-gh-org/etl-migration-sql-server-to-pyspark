#!/usr/bin/env python3
"""Fuzz + parity harness for mfg_lake.jobs.daily_production.

    python tools/fuzz_daily_production.py [--n 25] [--seed 20251117]

(a) generates N randomized mes.production_count variants (random counts,
    buckets straddling 06:00 local and shift edges, DST transition days incl.
    a spring-forward calendar window, NULL / zero totals, duplicate buckets,
    unknown SKUs / lines, out-of-window buckets);
(b) runs the job on each variant in a throwaway namespace and checks
    invariants plus an independent pure-Python oracle of the legacy procs;
(c) for the canonical seed: reconcile, byte-compare against
    legacy_snapshots/rpt.daily_production.csv, rerun determinism, earlier
    cutoff, corrupted-value negative control;
(d) writes everything to a structured JSON log (default
    out/validation/fuzz_daily_production.json). Exit 1 on any failure.
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
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "lakehouse" / "src"))
sys.path.insert(0, str(ROOT / "tools"))
from mfg_lake.common.paths import LAKE_ROOT, curated_dir  # noqa: E402
from mfg_lake.common.tz import windows_to_iana  # noqa: E402

RAW = ROOT / "data" / "raw"
SNAPSHOT = ROOT / "legacy_snapshots" / "rpt.daily_production.csv"
REPORT = "daily_production"
CANON_AS_OF = "2025-11-17 00:00:00"
KEYS = ["plant_id", "line_id", "production_day", "sku_id"]
COLUMNS = KEYS + ["total_units", "good_units", "cases", "yield_pct"]
SCHEMA = {"plant_id": "string", "line_id": "string", "production_day": "date32[day]",
          "sku_id": "string", "total_units": "int32", "good_units": "int32",
          "cases": "int32", "yield_pct": "decimal128(9, 2)"}
DIMS = ["dim.plant", "dim.line", "dim.sku", "dim.shift_pattern", "dim.calendar"]
TS = "%Y-%m-%d %H:%M:%S"
# DST transitions inside the two calendar windows used by variants
FALL_BACK = {"America/Chicago": date(2025, 11, 2), "America/New_York": date(2025, 11, 2),
             "America/Los_Angeles": date(2025, 11, 2), "Europe/London": date(2025, 10, 26)}
SPRING_FWD = {"America/Chicago": date(2025, 3, 9), "America/New_York": date(2025, 3, 9),
              "America/Los_Angeles": date(2025, 3, 9), "Europe/London": date(2025, 3, 30)}
SPRING_WINDOW = (date(2025, 3, 3), 35)  # 2025-03-03 .. 2025-04-06


def _read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


# ------------------------------------------------------------------ dims
class Dims:
    def __init__(self, raw_dir):
        self.plants = {r["plant_id"]: r for r in _read_csv(raw_dir / "dim.plant.csv")}
        self.tz = {p: windows_to_iana(r["tz_name"]) for p, r in self.plants.items()}
        self.lines = {r["line_id"]: r["plant_id"] for r in _read_csv(raw_dir / "dim.line.csv")}
        self.pack = {r["sku_id"]: int(r["pack_size"]) for r in _read_csv(raw_dir / "dim.sku.csv")}
        self.patterns = _read_csv(raw_dir / "dim.shift_pattern.csv")
        self.calendar = [date.fromisoformat(r["calendar_date"])
                         for r in _read_csv(raw_dir / "dim.calendar.csv")]


def _local_to_utc(naive, iana):
    # fold=0: ambiguous -> first (DST) offset, gap -> shifted forward (AT TIME ZONE)
    return naive.replace(tzinfo=ZoneInfo(iana)).astimezone(timezone.utc).replace(tzinfo=None)


def _hms(t):
    p = [int(x) for x in t.split(":")]
    return timedelta(hours=p[0], minutes=p[1], seconds=p[2] if len(p) > 2 else 0)


def oracle_shift_calendar(d: Dims):
    """Pure-python dim.usp_refresh_shift_calendar."""
    out = defaultdict(list)
    for sp in d.patterns:
        iana = d.tz[sp["plant_id"]]
        for day in d.calendar:
            base = datetime(day.year, day.month, day.day)
            s = _local_to_utc(base + _hms(sp["local_start"]), iana)
            e = _local_to_utc(base + timedelta(days=int(sp["end_next_day"]))
                              + _hms(sp["local_end"]), iana)
            out[sp["plant_id"]].append((s, e, sp["shift_code"], day))
    return out


def _round_half_away(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def oracle_report(d: Dims, counts):
    """Pure-python mes.usp_stg_production_counts + rpt.usp_rpt_daily_production."""
    sc = oracle_shift_calendar(d)
    agg = defaultdict(lambda: [0, 0])
    for r in counts:
        if any(r[c] in ("", None) for c in ("line_id", "sku_id", "bucket_start_utc",
                                              "bucket_end_utc", "total_units", "good_units")):
            continue  # NOT NULL contract
        plant = d.lines.get(r["line_id"])
        if plant is None or r["sku_id"] not in d.pack:
            continue
        bs = datetime.strptime(r["bucket_start_utc"], TS)
        local = bs.replace(tzinfo=timezone.utc).astimezone(ZoneInfo(d.tz[plant]))
        pday = (local.replace(tzinfo=None) - timedelta(hours=6)).date()
        n_match = sum(1 for s, e, _, _ in sc[plant] if s <= bs < e)
        for _ in range(n_match):
            a = agg[(plant, r["line_id"], pday, r["sku_id"])]
            a[0] += int(r["total_units"])
            a[1] += int(r["good_units"])
    rows = {}
    for (plant, line, pday, sku), (t, g) in agg.items():
        y = None if t == 0 else _round_half_away(Decimal(g) * 100 / Decimal(t))
        rows[(plant, line, pday, sku)] = (t, g, int(g / d.pack[sku]), y)
    return rows


# -------------------------------------------------------------- variants
def gen_variant(rng: random.Random, d: Dims, idx: int):
    """Returns (production_count rows, calendar rows or None, feature counts)."""
    spring = idx % 4 == 3  # every 4th variant uses a spring-forward calendar window
    cal_rows = None
    cal = d.calendar
    if spring:
        start, n = SPRING_WINDOW
        cal = [start + timedelta(days=i) for i in range(n)]
        cal_rows = [(c.isoformat(), c.isocalendar()[1], c.isoweekday(), c.strftime("%A"))
                    for c in cal]
    lines = rng.sample(sorted(d.lines), rng.randint(4, 10))
    skus = sorted(d.pack)
    feats = defaultdict(int)
    rows = []

    def add(line, sku, start_utc, minutes, total=None, good=None, tag="random"):
        if total is None:
            total = rng.choice([0] * 3 + [rng.randint(0, 5000) for _ in range(30)])
        if good is None:
            good = 0 if total == 0 else rng.randint(max(0, int(total * 0.9)), total)
        end = start_utc + timedelta(minutes=minutes, seconds=rng.choice([0, 0, 0, 30]))
        rows.append({"line_id": line, "sku_id": sku, "bucket_start_utc": start_utc.strftime(TS),
                     "bucket_end_utc": end.strftime(TS), "total_units": total,
                     "good_units": good})
        feats[tag] += 1

    for line in lines:
        plant = d.lines[line]
        iana = d.tz[plant]
        sku = rng.choice(skus)
        days = rng.sample(cal, min(len(cal), rng.randint(3, 8)))
        # DST transition day for this plant if one falls in the window
        tr = (SPRING_FWD if spring else FALL_BACK).get(iana)
        if tr and tr in cal:
            days.append(tr - timedelta(days=1))
            days.append(tr)
        for day in days:
            base = datetime(day.year, day.month, day.day)
            # regular aligned buckets over the production day (subsample)
            for b in rng.sample(range(96), rng.randint(5, 20)):
                add(line, sku, _local_to_utc(base + timedelta(hours=6, minutes=15 * b), iana),
                    15)
            # straddling 06:00 local, and exactly at / one second before 06:00
            a = _local_to_utc(base + timedelta(hours=6), iana)
            add(line, sku, a - timedelta(minutes=rng.randint(1, 14)), 15, tag="straddle_0600")
            add(line, sku, a, 15, tag="at_0600")
            add(line, sku, a - timedelta(seconds=1), 15, tag="just_before_0600")
            # straddling every shift edge of this plant
            for sp in d.patterns:
                if sp["plant_id"] != plant:
                    continue
                edge = _local_to_utc(base + _hms(sp["local_start"]), iana)
                add(line, sku, edge - timedelta(minutes=rng.randint(1, 14)), 15,
                    tag="straddle_shift_edge")
            if day == tr:
                # the transition hour itself: UTC-stepped buckets 00:00-04:00 local
                t0 = _local_to_utc(base, iana)
                for k in range(0, 5 * 60, rng.choice([15, 20, 30])):
                    add(line, sku, t0 + timedelta(minutes=k), 15, tag="dst_transition")
        # zero / NULL totals, duplicates, unknown sku / line
        for _ in range(rng.randint(1, 4)):
            day = rng.choice(cal)
            st = _local_to_utc(datetime(day.year, day.month, day.day, rng.randint(6, 23)), iana)
            add(line, sku, st, 15, total=0, good=0, tag="zero_total")
        for _ in range(rng.randint(0, 3)):
            r = dict(rng.choice(rows))
            r[rng.choice(["total_units", "good_units", "sku_id", "bucket_start_utc"])] = ""
            rows.append(r)
            feats["null_field"] += 1
        for _ in range(rng.randint(1, 5)):
            rows.append(dict(rng.choice(rows)))
            feats["duplicate_bucket"] += 1
        for _ in range(rng.randint(1, 3)):
            r = dict(rng.choice(rows))
            r["sku_id"] = f"SKU-ZZ{rng.randint(10, 99)}"
            rows.append(r)
            feats["unknown_sku"] += 1
        # outside the shift-calendar window (dropped by the inner range join)
        first = _local_to_utc(datetime.combine(cal[0], datetime.min.time())
                              + timedelta(hours=6), iana)
        last = _local_to_utc(datetime.combine(cal[-1], datetime.min.time())
                             + timedelta(days=1, hours=6), iana)
        add(line, sku, first - timedelta(minutes=rng.randint(1, 600)), 15, tag="before_window")
        add(line, sku, last + timedelta(minutes=rng.randint(0, 600)), 15, tag="after_window")
    rows.append(dict(rows[0], line_id="PLT99-L9"))
    feats["unknown_line"] += 1
    rng.shuffle(rows)
    return rows, cal_rows, dict(feats), ("spring_forward" if spring else "canonical_fall_back")


# ------------------------------------------------------------ invariants
def check_output(df: pd.DataFrame, schema, d: Dims, oracle, counts):
    """Returns {invariant: {"pass": bool, "detail": str}}."""
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
    put("good_le_total", [f"{r['line_id']} {r['production_day']}: good {r['good_units']} > "
                          f"total {r['total_units']}" for r in recs
                          if r["good_units"] > r["total_units"]])
    put("cases_eq_floor_good_div_pack",
        [f"{r['line_id']} {r['production_day']} {r['sku_id']}: cases {r['cases']} != "
         f"{r['good_units'] // d.pack[r['sku_id']]}" for r in recs
         if r["sku_id"] in d.pack and r["cases"] != r["good_units"] // d.pack[r["sku_id"]]])
    bad = []
    for r in recs:
        y = r["yield_pct"]
        y = None if y is None or (isinstance(y, float) and pd.isna(y)) else Decimal(str(y))
        if r["total_units"] == 0:
            if y is not None:
                bad.append(f"{r['line_id']} {r['production_day']}: yield {y} with total 0")
        elif y is None or not (0 <= y <= 100):
            bad.append(f"{r['line_id']} {r['production_day']}: yield {y} out of [0,100]")
        elif y != _round_half_away(Decimal(r["good_units"]) * 100 / Decimal(r["total_units"])):
            bad.append(f"{r['line_id']} {r['production_day']}: yield {y} not T-SQL ROUND")
    put("yield_range_null_and_rounding", bad)
    put("no_unknown_skus", [f"sku {r['sku_id']} not in dim.sku" for r in recs
                            if r["sku_id"] not in d.pack])
    put("no_unknown_lines", [f"line {r['line_id']}" for r in recs if r["line_id"] not in d.lines])
    # production_day: every output key is exactly the set the local-time rule
    # (date(local(bucket_start) - 6h)) yields for in-calendar buckets
    got = {(r["plant_id"], r["line_id"], r["production_day"], r["sku_id"]):
           (int(r["total_units"]), int(r["good_units"]), int(r["cases"]),
            None if r["yield_pct"] is None else Decimal(str(r["yield_pct"]))) for r in recs}
    put("production_day_local_rule", [f"missing {k}" for k in sorted(set(oracle) - set(got))]
        + [f"unexpected {k}" for k in sorted(set(got) - set(oracle))])
    put("oracle_values", [f"{k}: job {got[k]} oracle {oracle[k]}" for k in sorted(set(got) & set(oracle))
                          if got[k] != oracle[k]])
    n_null = sum(1 for r in counts if any(r[c] in ("", None) for c in
                                          ("sku_id", "bucket_start_utc", "total_units", "good_units")))
    res["_inputs"] = {"rows": len(counts), "null_rows": n_null, "output_rows": len(recs)}
    return res


def load_lake(ns):
    d = curated_dir(REPORT, ns)
    tbl = pq.read_table(sorted(d.glob("*.parquet")))
    df = tbl.to_pandas()
    df["yield_pct"] = df["yield_pct"].astype(object).where(df["yield_pct"].notna(), None)
    return df, tbl.schema


# ------------------------------------------------------------- canonical
def to_bcp_csv(df: pd.DataFrame) -> bytes:
    """Render like the MANIFEST bcp export (header, NULL = empty, ORDER BY key).
    The committed extract uses CRLF row terminators."""
    out = [",".join(COLUMNS)]
    for r in df.sort_values(KEYS).to_dict("records"):
        y = r["yield_pct"]
        out.append(",".join([r["plant_id"], r["line_id"], r["production_day"].isoformat(),
                             r["sku_id"], str(r["total_units"]), str(r["good_units"]),
                             "" if r["cases"] is None else str(r["cases"]),
                             "" if y is None else f"{Decimal(str(y)):.2f}"]))
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
    out["schema"] = check_output(df, schema, d, oracle_report(d, _read_csv(RAW / "mes.production_count.csv")),
                                 _read_csv(RAW / "mes.production_count.csv"))
    log(f"  canonical reconcile={'PASS' if rc == 0 else 'FAIL'} "
        f"byte_compare={'PASS' if mine == snap else 'FAIL'} sha256={_sha(mine)[:16]}")
    # rerun into the same namespace -> identical dataframe + dtypes
    job.run(ns, CANON_AS_OF, spark)
    df2, _ = load_lake(ns)
    out["rerun_deterministic"] = {"pass": _frames_equal(df, df2) and to_bcp_csv(df2) == mine}
    log(f"  rerun deterministic={out['rerun_deterministic']['pass']}")
    # earlier cutoff: legacy ignores AsOfUtc -> output must be identical
    early = "2025-11-03 00:00:00"
    ns_e = f"fuzz-{tag}-early"
    job.run(ns_e, early, spark)
    df3, _ = load_lake(ns_e)
    rc3, txt3 = run_reconcile(ns_e)
    out["earlier_cutoff"] = {"as_of_utc": early, "identical_to_canonical": _frames_equal(df, df3),
                             "reconcile_exit_code": rc3,
                             "pass": _frames_equal(df, df3) and rc3 == 0,
                             "note": "as-of-utc is accepted but not applied (legacy parity); "
                                     "KAN-6 AC4 expects a failure here - see docs/migration"}
    log(f"  earlier cutoff {early}: identical={out['earlier_cutoff']['identical_to_canonical']} "
        f"reconcile_exit={rc3}")
    # negative control: one corrupted value must fail reconcile
    ns_c = f"fuzz-{tag}-corrupt"
    src, dst = curated_dir(REPORT, ns), curated_dir(REPORT, ns_c)
    shutil.rmtree(dst, ignore_errors=True)
    dst.mkdir(parents=True)
    tbl = pq.read_table(sorted(src.glob("*.parquet")))
    import pyarrow as pa
    tu = tbl.column("total_units").to_pylist()
    tu[0] += 1
    tbl = tbl.set_column(tbl.schema.get_field_index("total_units"), "total_units",
                         pa.array(tu, type=pa.int32()))
    pq.write_table(tbl, dst / "part-00000.parquet")
    rc4, txt4 = run_reconcile(ns_c)
    out["corrupted_value"] = {"pass": rc4 != 0, "reconcile_exit_code": rc4,
                              "mutation": "row 0 total_units += 1",
                              "output_tail": "\n".join(txt4.strip().splitlines()[-3:])}
    log(f"  corrupted value -> reconcile exit {rc4} ({'expected FAIL' if rc4 else 'UNEXPECTED PASS'})")
    return out, df


def checker_self_test(df, schema, d, oracle, counts):
    """Mutate a passing output; each mutant must trip at least one invariant."""
    muts = {
        "cases_off_by_one": lambda x: x.assign(cases=x["cases"].where(x.index != 0, x["cases"] + 1)),
        "good_gt_total": lambda x: x.assign(good_units=x["good_units"].where(
            x.index != 0, x["total_units"] + 1)),
        # a 98.125 tie rounded half-to-even (98.12) instead of T-SQL ROUND (98.13)
        "bankers_rounding": lambda x: x.assign(
            total_units=x["total_units"].where(x.index != 0, 160),
            good_units=x["good_units"].where(x.index != 0, 157),
            yield_pct=[Decimal("98.12") if i == 0 else y for i, y in enumerate(x["yield_pct"])]),
        "production_day_shift": lambda x: x.assign(production_day=[
            p + timedelta(days=1) if i == 0 else p for i, p in enumerate(x["production_day"])]),
        "unknown_sku_leak": lambda x: pd.concat([x, x.head(1).assign(sku_id="SKU-ZZ00")],
                                                ignore_index=True),
    }
    out = {}
    for name, f in muts.items():
        r = check_output(f(df.copy()), schema, d, oracle, counts)
        tripped = [k for k, v in r.items() if not k.startswith("_") and not v["pass"]]
        out[name] = {"caught": bool(tripped), "tripped": tripped}
    return out


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=25)
    ap.add_argument("--seed", type=int, default=20251117)
    ap.add_argument("--log", default=str(ROOT / "out" / "validation" / "fuzz_daily_production.json"))
    ap.add_argument("--keep", action="store_true", help="keep throwaway namespaces")
    args = ap.parse_args()

    from mfg_lake.common.spark import get_spark
    from mfg_lake.jobs import daily_production as job

    def log(msg):
        print(msg, flush=True)

    spark = get_spark("fuzz_daily_production")
    spark.sparkContext.setLogLevel("ERROR")
    tag = f"{args.seed}"
    result = {"tool": "tools/fuzz_daily_production.py", "environment": env_info(spark, args)}
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
        counts, cal_rows, feats, window = gen_variant(rng, d0, i)
        if cal_rows:
            _write_csv(vdir / "dim.calendar.csv",
                       ["calendar_date", "iso_week", "day_of_week", "day_name"], cal_rows)
        _write_csv(vdir / "mes.production_count.csv",
                   ["bucket_id", "line_id", "sku_id", "bucket_start_utc", "bucket_end_utc",
                    "total_units", "good_units"],
                   [(n + 1, r["line_id"], r["sku_id"], r["bucket_start_utc"],
                     r["bucket_end_utc"], r["total_units"], r["good_units"])
                    for n, r in enumerate(counts)])
        d = Dims(vdir)
        ns = f"fuzz-{tag}-v{i:03d}"
        os.environ["RAW_DIR"] = str(vdir)
        as_of = (datetime(2025, 11, 17) - timedelta(hours=rng.randint(0, 24 * 30))).strftime(TS)
        entry = {"variant": i, "rng_seed": args.seed * 1000 + i, "namespace": ns,
                 "calendar_window": window, "as_of_utc": as_of, "features": feats}
        try:
            job.run(ns, as_of, spark)
            df, schema = load_lake(ns)
            oracle = oracle_report(d, counts)
            inv = check_output(df, schema, d, oracle, counts)
            entry["inputs"] = inv.pop("_inputs")
            entry["invariants"] = inv
            if i == 0:
                result["checker_self_test"] = checker_self_test(df, schema, d, oracle, counts)
        except Exception as e:  # job crash is a failed variant, not a harness crash
            entry["invariants"] = {"job_ran": {"pass": False, "detail": f"{type(e).__name__}: {e}"}}
        entry["pass"] = all(v["pass"] for v in entry["invariants"].values())
        entry["violations"] = [k for k, v in entry["invariants"].items() if not v["pass"]]
        entry["seconds"] = round(time.time() - vt, 2)
        variants.append(entry)
        log(f"  variant {i:03d} [{window:<19}] rows={entry.get('inputs', {}).get('rows', '?'):>5} "
            f"out={entry.get('inputs', {}).get('output_rows', '?'):>4} "
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
          and c["corrupted_value"]["pass"] and all(m["caught"] for m in st.values()))
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
