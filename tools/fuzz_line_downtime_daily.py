#!/usr/bin/env python3
"""Fuzz + parity harness for mfg_lake.jobs.line_downtime_daily.

    python tools/fuzz_line_downtime_daily.py [--n 25] [--seed 20251117]

(a) generates N randomized mes.downtime_event variants (mixed durations,
    events straddling shift edges / 06:00 / minute boundaries, zero-length
    events, both DST transition directions incl. the repeated fall-back
    hour and the spring gap, open events, as-of cutoff cases, duplicates,
    unknown lines / reasons, NULL NOT NULL columns, out-of-window events),
    on the canonical calendar or a spring-forward window;
(b) runs the job on each variant in a throwaway namespace and checks
    invariants plus an independent pure-Python oracle of the legacy procs
    (including the stg.downtime_shift_seg multiset, the KAN-7 contract);
(c) for the canonical seed: reconcile, byte-compare against
    legacy_snapshots/rpt.line_downtime_daily.csv, rerun determinism,
    earlier cutoff (must FAIL: AsOfUtc is applied), corrupted-value
    negative control;
(d) writes everything to a structured JSON log (default
    out/validation/fuzz_line_downtime_daily.json). Exit 1 on any failure.
"""
import argparse
import csv
import hashlib
import json
import os
import platform
import random
import re
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
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
SNAPSHOT = ROOT / "legacy_snapshots" / "rpt.line_downtime_daily.csv"
REPORT = "line_downtime_daily"
CANON_AS_OF = "2025-11-17 00:00:00"
KEYS = ["plant_id", "line_id", "production_day", "shift_code",
        "reason_category", "planned_flag"]
COLUMNS = KEYS + ["event_count", "downtime_minutes"]
SCHEMA = {"plant_id": "string", "line_id": "string", "production_day": "date32[day]",
          "shift_code": "string", "reason_category": "string",
          "planned_flag": "bool", "event_count": "int32",
          "downtime_minutes": "int32"}
DIMS = ["dim.plant", "dim.line", "dim.downtime_reason", "dim.shift_pattern",
        "dim.calendar"]
NN_COLS = ("event_id", "line_id", "start_utc", "reason_code", "planned_flag")
TS = "%Y-%m-%d %H:%M:%S"
# DST transition days inside the two calendar windows used by variants.
# Plants not listed (Mexico City, Sao Paulo) observe no DST.
FALL_BACK = {"America/Chicago": date(2025, 11, 2), "America/New_York": date(2025, 11, 2),
             "America/Los_Angeles": date(2025, 11, 2), "Europe/London": date(2025, 10, 26)}
SPRING_FWD = {"America/Chicago": date(2026, 3, 8), "America/New_York": date(2026, 3, 8),
              "America/Los_Angeles": date(2026, 3, 8), "Europe/London": date(2026, 3, 29)}
SPRING_WINDOW = (date(2026, 3, 2), 35)  # 2026-03-02 .. 2026-04-05

REPORT_META = {
    "ticket": "KAN-8",
    "pipeline": "PL_Line_Downtime",
    "fuzz_input": "mes.downtime_event",
    "bcp_note": "ORDER BY key, NULL = empty, BIT as 1/0",
    "oracle_note_html": "<code>oracle_values</code> / <code>key_set_vs_oracle</code> compare every output row, and <code>stage_segments_vs_oracle</code> every <code>stg.downtime_shift_seg</code> row, against an independent pure-Python implementation of the three legacy procs (zoneinfo, local-time overlap join, minute-boundary DATEDIFF).",
    "as_of_header": "as_of_utc (applied)",
    "earlier_cutoff_label": "earlier cutoff {as_of_utc} must change output and FAIL reconcile",
    "earlier_cutoff_detail": "changed={changed_vs_canonical}, reconcile exit {reconcile_exit_code}; KAN-8 AC4 met",
    "parity_notes_html": [
        "<b>AsOfUtc (KAN-8 AC4 met).</b> Applied exactly where legacy applies it, in <code>mes.usp_stg_downtime_local</code>: <code>start_utc &lt; @AsOfUtc</code> drops events that start at or after the cutoff, and <code>ISNULL(end_utc, @AsOfUtc)</code> caps open events only. Closed events that end after the cutoff keep their real end. An earlier cutoff changes the report and reconcile fails.",
        "<b>DATEDIFF(MINUTE) runs on local wall-clock values</b>, as in <code>rpt.usp_rpt_line_downtime_daily</code>. A 9h fall-back night shift reports 480 minutes, not 540. A 7h spring-forward night shift also reports 480, not 420. An event that lies entirely inside the repeated fall-back hour (for example 01:50 CDT to 01:10 CST) still joins the night shift and reports <b>negative</b> minutes. This is a legacy bug, kept for parity and not fixed without approval.",
        "<b>Shift split.</b> Every event is split across each shift it overlaps on local bounds, with strict <code>&lt;</code> / <code>&gt;</code>, so a zero-length event on a shift edge gets no segment. <code>production_day</code> comes from the shift calendar (the date the shift starts), so 03:00 local belongs to the previous day's night shift.",
        "<b>No seed tuning.</b> A pure-Python oracle of the procs reproduces the extract byte-for-byte from the existing seed: 450 staged events, 520 segments, 515 rows. Edge cases such as shift-edge straddles, both DST directions, open events, duplicates, unknown keys and NULLs are covered by the unit tests and the fuzz variants.",
        "<b>Line terminator.</b> MANIFEST shows <code>bcp -r\\n</code>, but the committed extract uses CRLF. The byte-compare renders CRLF.",
        "<b>NULLs in NOT NULL columns</b> of <code>mes.downtime_event</code> cannot occur in legacy. Such rows are dropped and counted rather than failing the load. <b>INT overflow</b> raises, like SQL Server's arithmetic overflow. A <b>duplicate event_id</b> (a PK violation, also impossible in legacy) is counted once by <code>COUNT(DISTINCT)</code>, but its minutes are summed twice, as SQL would do.",
        "<b>Ambiguous / gap local times</b> follow java.time / zoneinfo fold=0, the documented AT TIME ZONE behaviour. This was not verified against a live SQL Server. The shift calendar is reused from <code>daily_production.build_shift_calendar</code> (PR #5).",
        "The ADF JSON in <code>lakehouse/adf/</code> was reviewed, not deployed. Nothing here ran on ADF or Databricks.",
    ],
}


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
        self.plants = {r["plant_id"].strip(): r for r in _read_csv(raw_dir / "dim.plant.csv")}
        self.tz = {p: windows_to_iana(r["tz_name"]) for p, r in self.plants.items()}
        self.lines = {r["line_id"].strip(): r["plant_id"].strip()
                      for r in _read_csv(raw_dir / "dim.line.csv")}
        self.reasons = {r["reason_code"].strip(): r["reason_category"].strip()
                        for r in _read_csv(raw_dir / "dim.downtime_reason.csv")}
        self.patterns = _read_csv(raw_dir / "dim.shift_pattern.csv")
        for sp in self.patterns:
            sp["plant_id"] = sp["plant_id"].strip()
            sp["shift_code"] = sp["shift_code"].strip()
        self.shift_codes = {p: sorted({sp["shift_code"] for sp in self.patterns
                                       if sp["plant_id"] == p}) for p in self.plants}
        self.calendar = [date.fromisoformat(r["calendar_date"])
                         for r in _read_csv(raw_dir / "dim.calendar.csv")]


def _local_to_utc(naive, iana, fold=0):
    # fold=0: ambiguous -> first (DST) offset, gap -> shifted forward (AT TIME ZONE)
    return naive.replace(tzinfo=ZoneInfo(iana), fold=fold).astimezone(
        timezone.utc).replace(tzinfo=None)


def _utc_to_local(utc, iana):
    return utc.replace(tzinfo=timezone.utc).astimezone(ZoneInfo(iana)).replace(tzinfo=None)


def _hms(t):
    p = [int(x) for x in t.split(":")]
    return timedelta(hours=p[0], minutes=p[1], seconds=p[2] if len(p) > 2 else 0)


_AS_OF_RE = re.compile(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.\d+)?Z?$")


def _parse_as_of(s):
    if isinstance(s, datetime):
        return s
    m = _AS_OF_RE.fullmatch(str(s).strip())
    if not m:
        raise ValueError(f"unparseable as_of {s!r}")
    return datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}")


# ------------------------------------------------------------------ oracle
# Pure-python ports of the three legacy procs (no Spark, no mfg_lake.jobs).

def oracle_shift_calendar(d: Dims):
    """dim.shift_calendar in LOCAL wall-clock bounds (as the split proc reads
    them: shift_calendar UTC bounds converted back via AT TIME ZONE)."""
    out = defaultdict(list)
    for sp in d.patterns:
        iana = d.tz[sp["plant_id"]]
        for day in d.calendar:
            base = datetime(day.year, day.month, day.day)
            su = _local_to_utc(base + _hms(sp["local_start"]), iana)
            eu = _local_to_utc(base + timedelta(days=int(sp["end_next_day"]))
                               + _hms(sp["local_end"]), iana)
            out[sp["plant_id"]].append(
                (day, sp["shift_code"], _utc_to_local(su, iana), _utc_to_local(eu, iana)))
    return out


def oracle_stage(d: Dims, events, as_of) -> list:
    """mes.usp_stg_downtime_local -> stg.downtime_local rows."""
    as_of = _parse_as_of(as_of)
    out = []
    for e in events:
        r = {k: (v.strip() if isinstance(v, str) else v) for k, v in e.items()}
        if any(r.get(c) in ("", None) for c in NN_COLS):
            continue  # NOT NULL contract
        plant = d.lines.get(r["line_id"])
        if plant is None or plant not in d.tz:
            continue  # inner join dim.line / dim.plant
        s = datetime.strptime(r["start_utc"], TS)
        if not s < as_of:
            continue
        en = datetime.strptime(r["end_utc"], TS) if r.get("end_utc") else as_of
        iana = d.tz[plant]
        out.append((int(r["event_id"]), plant, r["line_id"], r["reason_code"],
                    int(r["planned_flag"]), s, en,
                    _utc_to_local(s, iana), _utc_to_local(en, iana)))
    return out


def oracle_segments(d: Dims, stg) -> list:
    """mes.usp_split_downtime_by_shift -> stg.downtime_shift_seg rows
    (event_id, plant, line, reason, planned, day, code, seg_start, seg_end)."""
    sc = oracle_shift_calendar(d)
    out = []
    for (eid, p, l, rc, pf, _su, _eu, sl, el) in stg:
        for (day, code, ss, se) in sc[p]:
            if ss < el and se > sl:
                out.append((eid, p, l, rc, pf, day, code, max(sl, ss), min(el, se)))
    return out


def _diff_minute(a, b):
    epoch = datetime(2000, 1, 1)
    return int((b - epoch).total_seconds() // 60) - int((a - epoch).total_seconds() // 60)


def oracle_report(d: Dims, segs) -> dict:
    """rpt.usp_rpt_line_downtime_daily -> {key: (event_count, minutes)}."""
    agg = defaultdict(lambda: [set(), 0])
    for (eid, p, l, rc, pf, day, code, a, b) in segs:
        cat = d.reasons.get(rc)
        if cat is None:
            continue  # inner join dim.downtime_reason
        g = agg[(p, l, day, code, cat, pf)]
        g[0].add(eid)
        g[1] += _diff_minute(a, b)
    return {k: (len(v[0]), v[1]) for k, v in agg.items()}


# -------------------------------------------------------------- variants
def gen_variant(rng: random.Random, d: Dims, idx: int):
    """Returns (events rows, calendar rows or None, feature counts, window name,
    as_of_utc string)."""
    spring = idx % 4 == 3  # every 4th variant uses a spring-forward calendar window
    cal_rows = None
    cal = d.calendar
    if spring:
        start, n = SPRING_WINDOW
        cal = [start + timedelta(days=i) for i in range(n)]
        cal_rows = [(c.isoformat(), c.isocalendar()[1], c.isoweekday(), c.strftime("%A"))
                    for c in cal]
    transitions = SPRING_FWD if spring else FALL_BACK
    feats = defaultdict(int)
    rows = []
    seq = [0]

    def nid():
        seq[0] += 1
        return seq[0]

    def add(line, s_utc, e_utc, reason=None, planned=None, tag="random", eid=None):
        rows.append({"event_id": eid if eid is not None else nid(),
                     "line_id": line,
                     "start_utc": s_utc.strftime(TS),
                     "end_utc": "" if e_utc is None else e_utc.strftime(TS),
                     "reason_code": reason or rng.choice(sorted(d.reasons)),
                     "planned_flag": rng.randint(0, 1) if planned is None else planned})
        feats[tag] += 1

    # as_of: a random second in [window start+3d, window end+2d]
    lo = datetime(cal[0].year, cal[0].month, cal[0].day) + timedelta(days=3)
    hi = datetime(cal[-1].year, cal[-1].month, cal[-1].day) + timedelta(days=2)
    as_of = lo + timedelta(seconds=rng.randint(0, int((hi - lo).total_seconds())))
    as_of_s = as_of.strftime(TS)
    if idx % 5 == 4:
        as_of_s = as_of.strftime("%Y-%m-%dT%H:%M:%SZ")

    lines = rng.sample(sorted(d.lines), min(len(d.lines), rng.randint(4, 10)))
    for line in lines:
        plant = d.lines[line]
        iana = d.tz[plant]
        days = rng.sample(cal, min(len(cal), rng.randint(3, 8)))
        tr = transitions.get(iana)
        if tr and tr in cal:
            days.append(tr)
        win0_utc = _local_to_utc(datetime(cal[0].year, cal[0].month, cal[0].day), iana)
        win1_utc = _local_to_utc(datetime(cal[-1].year, cal[-1].month, cal[-1].day)
                                 + timedelta(days=1), iana)
        span_s = int((win1_utc - win0_utc).total_seconds())

        # random events: mixed durations, seconds jitter incl. :59
        for _ in range(rng.randint(8, 20)):
            s = win0_utc + timedelta(seconds=rng.randint(0, span_s))
            s += timedelta(seconds=rng.choice([0, 0, 0, 1, 30, 59]))
            if s >= win1_utc:
                s = win1_utc - timedelta(seconds=1)
            dur = rng.choice([timedelta(0),
                              timedelta(seconds=rng.randint(1, 59)),
                              timedelta(minutes=rng.randint(1, 180)),
                              timedelta(minutes=rng.randint(180, 30 * 60))])
            add(line, s, s + dur)

        # shift-edge straddles: each shift start of this plant, 2-5 random days
        starts = sorted({_hms(sp["local_start"]) for sp in d.patterns
                         if sp["plant_id"] == plant})
        for day in rng.sample(cal, min(len(cal), rng.randint(2, 5))):
            base = datetime(day.year, day.month, day.day)
            for edge_off in starts:
                edge = _local_to_utc(base + edge_off, iana)
                add(line, edge - timedelta(minutes=rng.randint(1, 90)),
                    edge + timedelta(minutes=rng.randint(1, 90)),
                    tag="straddle_shift_edge")
            a = _local_to_utc(base + timedelta(hours=6), iana)
            add(line, a - timedelta(minutes=rng.randint(1, 90)),
                a + timedelta(minutes=rng.randint(1, 90)), tag="straddle_0600")
            # minute boundary: local hh:mm:59 -> hh:mm+1:00 (DATEDIFF = 1)
            mb = base + timedelta(hours=rng.randint(0, 22), minutes=rng.randint(0, 59),
                                  seconds=59)
            add(line, _local_to_utc(mb, iana), _local_to_utc(mb + timedelta(seconds=1), iana),
                tag="minute_boundary")
            # zero-length inside a shift, and exactly on the edge (no segment)
            add(line, _local_to_utc(base + timedelta(hours=9), iana),
                _local_to_utc(base + timedelta(hours=9), iana), tag="zero_length_inside")
            add(line, a, a, tag="zero_length_on_edge")

        # DST transition day (plants with DST only)
        if tr and tr in cal:
            base = datetime(tr.year, tr.month, tr.day)
            s = _local_to_utc(base + timedelta(minutes=30), iana)
            e = _local_to_utc(base + timedelta(hours=2, minutes=30), iana)
            add(line, s, e, tag="dst_transition")  # local 00:30 -> 02:30
            if spring:
                # local 01:30 -> 03:30 crosses the 02:00-03:00 gap
                add(line, _local_to_utc(base + timedelta(hours=1, minutes=30), iana),
                    _local_to_utc(base + timedelta(hours=3, minutes=30), iana),
                    tag="dst_gap")
            else:
                # start in the first 01:xx (fold=0), end in the second (fold=1)
                # with end_local < start_local -> negative DATEDIFF
                m1, m2 = sorted(rng.sample(range(5, 55), 2))
                add(line, _local_to_utc(base + timedelta(hours=1, minutes=m2), iana, fold=0),
                    _local_to_utc(base + timedelta(hours=1, minutes=m1), iana, fold=1),
                    tag="dst_repeated_hour")

        # as-of cutoff behaviour
        add(line, as_of - timedelta(hours=rng.randint(1, 12)), None, tag="open_event")
        add(line, as_of - timedelta(hours=rng.randint(1, 6)),
            as_of + timedelta(hours=rng.randint(1, 6)), tag="ends_after_cutoff")
        add(line, as_of, as_of + timedelta(minutes=30), tag="starts_at_or_after_cutoff")
        for _ in range(rng.randint(0, 2)):
            add(line, as_of + timedelta(minutes=rng.randint(1, 600)),
                as_of + timedelta(minutes=rng.randint(700, 1200)),
                tag="starts_at_or_after_cutoff")

        # ends before the window's first shift for this plant
        first = min(_local_to_utc(datetime(cal[0].year, cal[0].month, cal[0].day)
                                  + _hms(sp["local_start"]), iana)
                    for sp in d.patterns if sp["plant_id"] == plant)
        end = first - timedelta(minutes=rng.randint(1, 300))
        add(line, end - timedelta(minutes=rng.randint(10, 120)), end, tag="before_window")

        # duplicates reuse the event_id (PK violation: counted once, minutes twice)
        for _ in range(rng.randint(1, 4)):
            rows.append(dict(rng.choice(rows)))
            feats["duplicate"] += 1

        # NULLs in NOT NULL columns (empty fields -> inferSchema NULL)
        for _ in range(rng.randint(1, 3)):
            r = dict(rng.choice(rows))
            r[rng.choice(NN_COLS)] = ""
            rows.append(r)
            feats["null_not_null_col"] += 1
        for c in NN_COLS:  # guarantee every NOT NULL column is exercised
            r = dict(rng.choice(rows))
            r[c] = ""
            rows.append(r)
            feats["null_not_null_col"] += 1

        add(line, win0_utc + timedelta(hours=12), win0_utc + timedelta(hours=13),
            tag="unknown_reason", reason=None)  # placeholder replaced below
        rows[-1]["reason_code"] = "R-ZZZ"
        feats["random"] -= 0  # no-op; tag already set

    r = dict(rows[rng.randrange(len(rows))])
    r["line_id"] = "ZZZ-L9"
    r["event_id"] = nid()
    rows.append(r)
    feats["unknown_line"] += 1
    rng.shuffle(rows)
    return rows, cal_rows, dict(feats), ("spring_forward" if spring else "canonical_fall_back"), as_of_s


# ------------------------------------------------------------ invariants
def check_output(df: pd.DataFrame, schema, d: Dims, oracle, events):
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
    put("no_nulls", [f"null in {c}" for c in COLUMNS if df[c].isna().any()])
    put("event_count_positive", [f"{r['line_id']} {r['production_day']}: "
                                 f"event_count {r['event_count']}" for r in recs
                                 if not r["event_count"] >= 1])
    put("no_unknown_lines", [f"line {r['line_id']}" for r in recs
                             if r["line_id"] not in d.lines])
    cats = set(d.reasons.values())
    put("no_unknown_reason_categories",
        [f"category {r['reason_category']}" for r in recs
         if r["reason_category"] not in cats])
    put("shift_code_valid_for_plant",
        [f"{r['plant_id']}/{r['shift_code']}" for r in recs
         if r["shift_code"] not in d.shift_codes.get(r["plant_id"], [])])
    calset = set(d.calendar)
    put("production_day_in_calendar",
        [f"{r['line_id']} {r['production_day']}" for r in recs
         if r["production_day"] not in calset])
    fb_ok = {p: {fb - timedelta(days=1), fb}
             for p, i in d.tz.items() for fb in [FALL_BACK.get(i)] if fb}
    put("negative_minutes_only_on_fall_back",
        [f"{r['plant_id']} {r['production_day']} {r['downtime_minutes']}" for r in recs
         if r["downtime_minutes"] < 0
         and r["production_day"] not in fb_ok.get(r["plant_id"], set())])
    got = {(r["plant_id"], r["line_id"], r["production_day"], r["shift_code"],
            r["reason_category"], int(bool(r["planned_flag"]))):
           (int(r["event_count"]), int(r["downtime_minutes"])) for r in recs}
    put("key_set_vs_oracle", [f"missing {k}" for k in sorted(set(oracle) - set(got))]
        + [f"unexpected {k}" for k in sorted(set(got) - set(oracle))])
    put("oracle_values", [f"{k}: job {got[k]} oracle {oracle[k]}"
                          for k in sorted(set(got) & set(oracle))
                          if got[k] != oracle[k]])
    res["_inputs"] = {
        "rows": len(events),
        "null_rows": sum(1 for r in events if any(r.get(c) in ("", None) for c in NN_COLS)),
        "open_events": sum(1 for r in events if r.get("end_utc") in ("", None)),
        "output_rows": len(recs)}
    return res


def stage_segments_vs_oracle(job, dp, spark, d, events, as_of):
    """KAN-7 contract: stage + split output multiset == oracle segments."""
    from mfg_lake.common.io import read_raw
    feeds = {t: read_raw(spark, t) for t in job.FEEDS}
    stg = job.stage_downtime_local(feeds["mes.downtime_event"], feeds["dim.line"],
                                   feeds["dim.plant"], as_of)
    sc = dp.build_shift_calendar(feeds["dim.plant"], feeds["dim.shift_pattern"],
                                 feeds["dim.calendar"])
    seg = job.split_downtime_by_shift(stg, feeds["dim.plant"], sc)
    got = Counter((r.event_id, r.plant_id, r.line_id, r.reason_code,
                   int(bool(r.planned_flag)), r.production_day, r.shift_code,
                   r.seg_start_local, r.seg_end_local) for r in seg.collect())
    exp = Counter(oracle_segments(d, oracle_stage(d, events, as_of)))
    diff = got - exp
    diff2 = exp - got
    detail = ("ok" if not diff and not diff2
              else f"extra={list(diff.items())[:3]} missing={list(diff2.items())[:3]}")
    return {"pass": not diff and not diff2,
            "detail": f"{detail} (job={sum(got.values())} oracle={sum(exp.values())})"}


def load_lake(ns):
    d = curated_dir(REPORT, ns)
    tbl = pq.read_table(sorted(d.glob("*.parquet")))
    return tbl.to_pandas(), tbl.schema


# ------------------------------------------------------------- canonical
def to_bcp_csv(df: pd.DataFrame) -> bytes:
    """Render like the MANIFEST bcp export (header, NULL = empty, ORDER BY key,
    BIT as 1/0). The committed extract uses CRLF row terminators."""
    out = [",".join(COLUMNS)]
    for r in df.sort_values(KEYS).to_dict("records"):
        out.append(",".join([r["plant_id"], r["line_id"],
                             r["production_day"].isoformat(), r["shift_code"],
                             r["reason_category"], "1" if r["planned_flag"] else "0",
                             str(r["event_count"]), str(r["downtime_minutes"])]))
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


def canonical_checks(job, dp, spark, tag, log):
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
    snap_keys = {",".join(l.split(",")[:6]) for l in snap_rows[1:]}
    mine_keys = {",".join(l.split(",")[:6]) for l in mine_rows[1:]}
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
    canon_events = _read_csv(RAW / "mes.downtime_event.csv")
    out["schema"] = check_output(
        df, schema, d,
        oracle_report(d, oracle_segments(d, oracle_stage(d, canon_events, CANON_AS_OF))),
        canon_events)
    out["schema"]["stage_segments_vs_oracle"] = stage_segments_vs_oracle(
        job, dp, spark, d, canon_events, CANON_AS_OF)
    log(f"  canonical reconcile={'PASS' if rc == 0 else 'FAIL'} "
        f"byte_compare={'PASS' if mine == snap else 'FAIL'} sha256={_sha(mine)[:16]}")
    # rerun into the same namespace -> identical dataframe + dtypes
    job.run(ns, CANON_AS_OF, spark)
    df2, _ = load_lake(ns)
    out["rerun_deterministic"] = {"pass": _frames_equal(df, df2) and to_bcp_csv(df2) == mine}
    log(f"  rerun deterministic={out['rerun_deterministic']['pass']}")
    # earlier cutoff: AsOfUtc IS applied -> output must change and reconcile fail
    early = "2025-11-10 00:00:00"
    ns_e = f"fuzz-{tag}-early"
    job.run(ns_e, early, spark)
    df3, _ = load_lake(ns_e)
    rc3, txt3 = run_reconcile(ns_e)
    changed = not _frames_equal(df, df3)
    out["earlier_cutoff"] = {"as_of_utc": early, "changed_vs_canonical": changed,
                             "identical_to_canonical": not changed,
                             "reconcile_exit_code": rc3,
                             "pass": changed and rc3 != 0,
                             "note": "AsOfUtc is applied as in mes.usp_stg_downtime_local; "
                                     "KAN-8 AC4: earlier cutoff must fail reconcile"}
    log(f"  earlier cutoff {early}: changed={changed} reconcile_exit={rc3}")
    # negative control: one corrupted value must fail reconcile
    ns_c = f"fuzz-{tag}-corrupt"
    src, dst = curated_dir(REPORT, ns), curated_dir(REPORT, ns_c)
    shutil.rmtree(dst, ignore_errors=True)
    dst.mkdir(parents=True)
    tbl = pq.read_table(sorted(src.glob("*.parquet")))
    import pyarrow as pa
    dm = tbl.column("downtime_minutes").to_pylist()
    dm[0] += 1
    tbl = tbl.set_column(tbl.schema.get_field_index("downtime_minutes"), "downtime_minutes",
                         pa.array(dm, type=pa.int32()))
    pq.write_table(tbl, dst / "part-00000.parquet")
    rc4, txt4 = run_reconcile(ns_c)
    out["corrupted_value"] = {"pass": rc4 != 0, "reconcile_exit_code": rc4,
                              "mutation": "row 0 downtime_minutes += 1",
                              "output_tail": "\n".join(txt4.strip().splitlines()[-3:])}
    log(f"  corrupted value -> reconcile exit {rc4} ({'expected FAIL' if rc4 else 'UNEXPECTED PASS'})")
    return out, df


def checker_self_test(df, schema, d, oracle, events):
    """Mutate a passing output; each mutant must trip at least one invariant."""
    non_fb = df.index[df["production_day"].apply(
        lambda p: p not in {fbd for s in {FALL_BACK.get(tz) for tz in d.tz.values()
                                          if FALL_BACK.get(tz)}
                            for fbd in (s - timedelta(days=1), s)})]
    other_shift = {p: codes for p, codes in d.shift_codes.items() if len(codes) > 1}
    i_swap = next(i for i in df.index
                  if df.at[i, "plant_id"] in other_shift)
    muts = {
        "minutes_off_by_one": lambda x: x.assign(downtime_minutes=x["downtime_minutes"].where(
            x.index != 0, x["downtime_minutes"] + 1)),
        "event_count_plus_one": lambda x: x.assign(event_count=x["event_count"].where(
            x.index != 0, x["event_count"] + 1)),
        "production_day_shift": lambda x: x.assign(production_day=[
            p + timedelta(days=1) if i == 0 else p for i, p in enumerate(x["production_day"])]),
        "planned_flag_flip": lambda x: x.assign(planned_flag=[
            not v if i == 0 else v for i, v in enumerate(x["planned_flag"])]),
        "shift_code_swap": lambda x: x.assign(shift_code=[
            next(c for c in other_shift[x.at[i, "plant_id"]] if c != x.at[i, "shift_code"])
            if i == i_swap else v for i, v in enumerate(x["shift_code"])]),
        "unknown_reason_leak": lambda x: x.assign(reason_category=x["reason_category"].where(
            x.index != 0, "Unknown")),
        "unknown_line_leak": lambda x: x.assign(line_id=x["line_id"].where(
            x.index != 0, "ZZZ-L9")),
        "duplicate_key_row": lambda x: pd.concat([x, x.head(1)], ignore_index=True),
        "negative_minutes_outside_fall_back": lambda x: x.assign(
            downtime_minutes=x["downtime_minutes"].where(
                x.index != non_fb[0], -5)),
        "event_count_int64_dtype": lambda x: x.assign(
            event_count=x["event_count"].astype("int64")),
    }
    out = {}
    for name, f in muts.items():
        if name == "event_count_int64_dtype":
            mut = f(df.copy())
            got = {f_.name: str(f_.type) for f_ in pa_schema_like(mut)}
            res = {"schema_contract": {"pass": False if got["event_count"] != SCHEMA["event_count"] else True,
                                       "detail": "int64"}}
            out[name] = {"caught": got["event_count"] != SCHEMA["event_count"],
                         "tripped": ["schema_contract"] if got["event_count"] != SCHEMA["event_count"] else []}
            continue
        r = check_output(f(df.copy()), schema, d, oracle, events)
        tripped = [k for k, v in r.items() if not k.startswith("_") and not v["pass"]]
        out[name] = {"caught": bool(tripped), "tripped": tripped}
    return out


def pa_schema_like(df):
    import pyarrow as pa
    return pa.Table.from_pandas(df, preserve_index=False).schema


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
    ap.add_argument("--log", default=str(ROOT / "out" / "validation" / "fuzz_line_downtime_daily.json"))
    ap.add_argument("--keep", action="store_true", help="keep throwaway namespaces")
    args = ap.parse_args()

    from mfg_lake.common.spark import get_spark
    from mfg_lake.jobs import daily_production as dp
    from mfg_lake.jobs import line_downtime_daily as job

    def log(msg):
        print(msg, flush=True)

    # oracle sanity: canonical seed must reproduce the extract byte-for-byte
    d0 = Dims(RAW)
    canon_events = _read_csv(RAW / "mes.downtime_event.csv")
    canon_segs = oracle_segments(d0, oracle_stage(d0, canon_events, CANON_AS_OF))
    canon_oracle = oracle_report(d0, canon_segs)
    orows = [",".join(COLUMNS)] + [",".join(map(str, k)) + f",{v[0]},{v[1]}"
                                 for k, v in sorted(canon_oracle.items())]
    odata = ("\r\n".join(orows) + "\r\n").encode()
    assert odata == SNAPSHOT.read_bytes(), "oracle does not reproduce the snapshot"
    early_oracle = oracle_report(d0, oracle_segments(
        d0, oracle_stage(d0, canon_events, "2025-11-10 00:00:00")))
    assert len(early_oracle) == 421, f"oracle@2025-11-10 gives {len(early_oracle)} rows != 421"

    spark = get_spark("fuzz_line_downtime_daily")
    spark.sparkContext.setLogLevel("ERROR")
    tag = f"{args.seed}"
    result = {"tool": "tools/fuzz_line_downtime_daily.py", "report_meta": REPORT_META,
              "environment": env_info(spark, args)}
    t0 = time.time()

    log(f"== canonical seed (AS_OF_UTC='{CANON_AS_OF}') ==")
    result["canonical"], canon_df = canonical_checks(job, dp, spark, tag, log)

    variants = []
    work = LAKE_ROOT / f"fuzz-{tag}-raw"
    for i in range(args.n):
        vt = time.time()
        rng = random.Random(args.seed * 1000 + i)
        vdir = work / f"v{i:03d}"
        shutil.rmtree(vdir, ignore_errors=True)
        vdir.mkdir(parents=True)
        for t in DIMS:
            shutil.copy(RAW / f"{t}.csv", vdir / f"{t}.csv")
        events, cal_rows, feats, window, as_of = gen_variant(rng, d0, i)
        if cal_rows:
            _write_csv(vdir / "dim.calendar.csv",
                       ["calendar_date", "iso_week", "day_of_week", "day_name"], cal_rows)
        _write_csv(vdir / "mes.downtime_event.csv",
                   ["event_id", "line_id", "start_utc", "end_utc", "reason_code",
                    "planned_flag"],
                   [(r["event_id"], r["line_id"], r["start_utc"], r["end_utc"],
                     r["reason_code"], r["planned_flag"]) for r in events])
        d = Dims(vdir)
        ns = f"fuzz-{tag}-v{i:03d}"
        os.environ["RAW_DIR"] = str(vdir)
        entry = {"variant": i, "rng_seed": args.seed * 1000 + i, "namespace": ns,
                 "calendar_window": window, "as_of_utc": as_of, "features": feats}
        try:
            job.run(ns, as_of, spark)
            df, schema = load_lake(ns)
            segs = oracle_segments(d, oracle_stage(d, events, as_of))
            oracle = oracle_report(d, segs)
            inv = check_output(df, schema, d, oracle, events)
            inv["stage_segments_vs_oracle"] = stage_segments_vs_oracle(
                job, dp, spark, d, events, as_of)
            entry["inputs"] = inv.pop("_inputs")
            entry["invariants"] = inv
            if i == 0:
                result["checker_self_test"] = checker_self_test(df, schema, d, oracle, events)
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
        "earlier_cutoff_fails_reconcile": c["earlier_cutoff"]["pass"],
        "corrupted_value_detected": c["corrupted_value"]["pass"],
        "checker_mutants_caught": f"{sum(m['caught'] for m in st.values())}/{len(st)}",
        "seconds": round(time.time() - t0, 1),
    }
    ok = (all(v["pass"] for v in variants) and c["reconcile"]["pass"]
          and c["byte_compare"]["pass"] and c["rerun_deterministic"]["pass"]
          and c["earlier_cutoff"]["pass"] and c["corrupted_value"]["pass"]
          and c["schema"]["stage_segments_vs_oracle"]["pass"]
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
