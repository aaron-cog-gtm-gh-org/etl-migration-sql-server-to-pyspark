#!/usr/bin/env python3
"""Pure-Python oracle of the legacy PL_OEE data flow (KAN-7).

Independent of Spark: zoneinfo for AT TIME ZONE, Decimal for SQL Server
DECIMAL arithmetic. Re-implements, from legacy/sql/procs/*.sql:

  dim.usp_refresh_shift_calendar  -> shift_calendar()
  mes.usp_stg_downtime_local      -> downtime_local()      (uses AsOfUtc)
  mes.usp_split_downtime_by_shift -> downtime_shift_seg()
  mes.usp_stg_production_counts   -> production_local()
  mes.usp_calc_planned_time       -> planned_time()
  rpt.usp_rpt_oee_shift           -> oee_shift()

    python tools/oracle_oee_shift.py [--raw-dir DIR] [--as-of-utc TS] [--mode tsql]
                                     [--compare legacy_snapshots/rpt.oee_shift.csv]

SQL Server DECIMAL typing of the rpt.usp_rpt_oee_shift expressions
(p,s per the T-SQL precision/scale rules, capped at 38):

  A  = CAST(shift - unplanned AS DECIMAL(18,4)) / NULLIF(shift, 0)   -> (29,15)
  P  = CAST(total AS DECIMAL(18,4)) / NULLIF(ideal DECIMAL(38,2), 0) -> (38,22)
  Q  = CAST(good AS DECIMAL(18,4)) / NULLIF(total, 0)                -> (29,15)
  oee chain, left to right:
    X1 = A                                  (29,15)
    X2 = X1 * CAST(total AS DECIMAL(18,4))  (38,9)   scale reduced
    X3 = X2 / NULLIF(ideal, 0)              (38,7)   scale reduced
    X4 = X3 * CAST(good AS DECIMAL(18,4))   (38,6)   scale reduced
    X5 = X4 / NULLIF(total, 0)              (38,6)
  each metric = ROUND(x, 4) (half away from zero) -> DECIMAL(9,4)

mode: how a result is fitted to its scale.
  tsql  : division truncates, multiplication rounds half away from zero
  round : every step rounds half away from zero (Spark decimal semantics)
  exact : no intermediate scale limits (exact rationals), ROUND at the end
"""
import argparse
import csv
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal, localcontext
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "lakehouse" / "src"))
from mfg_lake.common.tz import windows_to_iana  # noqa: E402  (pure-Python dict lookup)

LEGACY_MIN_PRODUCTION_DAY = date(2025, 10, 20)  # rpt.usp_rpt_oee_shift WHERE clause
COLUMNS = ("plant_id", "line_id", "production_day", "shift_code", "planned_min",
           "unplanned_dt_min", "availability", "performance", "quality", "oee")
TS = "%Y-%m-%d %H:%M:%S"


def _read(raw_dir, table):
    with open(Path(raw_dir) / f"{table}.csv", newline="") as f:
        return list(csv.DictReader(f))


def _ts(s):
    s = (s or "").strip()
    return datetime.strptime(s, TS) if s else None


def _int(s):
    s = (s or "").strip()
    return int(s) if s else None


def local_to_utc(naive, iana):
    # fold=0: ambiguous -> earlier (DST) offset; gap -> shifted forward
    return naive.replace(tzinfo=ZoneInfo(iana), fold=0).astimezone(timezone.utc).replace(tzinfo=None)


def utc_to_local(utc, iana):
    return utc.replace(tzinfo=timezone.utc).astimezone(ZoneInfo(iana)).replace(tzinfo=None)


def datediff_minute(a, b):
    """DATEDIFF(MINUTE, a, b): minute boundaries crossed."""
    epoch = datetime(1970, 1, 1)
    return int((b - epoch).total_seconds() // 60) - int((a - epoch).total_seconds() // 60)


def _hms(t):
    parts = [int(x) for x in t.strip().split(":")]
    return timedelta(hours=parts[0], minutes=parts[1], seconds=parts[2] if len(parts) > 2 else 0)


class Raw:
    def __init__(self, raw_dir):
        self.plant_tz = {r["plant_id"].strip(): windows_to_iana(r["tz_name"])
                         for r in _read(raw_dir, "dim.plant")}
        self.line_plant = {r["line_id"].strip(): r["plant_id"].strip()
                           for r in _read(raw_dir, "dim.line")}
        self.patterns = _read(raw_dir, "dim.shift_pattern")
        self.calendar = [date.fromisoformat(r["calendar_date"].strip())
                         for r in _read(raw_dir, "dim.calendar")]
        self.sku_rate = {r["sku_id"].strip(): Decimal(r["ideal_units_per_min"].strip()).quantize(Decimal("0.01"), ROUND_HALF_UP)
                         for r in _read(raw_dir, "dim.sku")}
        self.events = _read(raw_dir, "mes.downtime_event")
        self.buckets = _read(raw_dir, "mes.production_count")


def shift_calendar(raw):
    """dim.usp_refresh_shift_calendar over every dim.calendar date (WindowDays covers it)."""
    out = []
    for sp in raw.patterns:
        p = sp["plant_id"].strip()
        if p not in raw.plant_tz:
            continue
        tz = raw.plant_tz[p]
        for d in raw.calendar:
            d0 = datetime(d.year, d.month, d.day)
            ls = d0 + _hms(sp["local_start"])
            le = d0 + timedelta(days=int(sp["end_next_day"])) + _hms(sp["local_end"])
            out.append({"plant_id": p, "shift_code": sp["shift_code"].strip(), "production_day": d,
                        "start_utc": local_to_utc(ls, tz), "end_utc": local_to_utc(le, tz)})
    return out


def downtime_local(raw, as_of):
    """mes.usp_stg_downtime_local(@AsOfUtc): NOT NULL rows only (legacy contract),
    JOIN dim.line / dim.plant, start_utc < @AsOfUtc, ISNULL(end_utc, @AsOfUtc)."""
    out = []
    for e in raw.events:
        eid, line = _int(e["event_id"]), (e["line_id"] or "").strip()
        start, end = _ts(e["start_utc"]), _ts(e["end_utc"])
        reason, flag = (e["reason_code"] or "").strip(), (e["planned_flag"] or "").strip()
        if eid is None or not line or start is None or not reason or flag == "":
            continue
        plant = raw.line_plant.get(line)
        if plant is None or plant not in raw.plant_tz or not start < as_of:
            continue
        end = end if end is not None else as_of
        tz = raw.plant_tz[plant]
        out.append({"event_id": eid, "plant_id": plant, "line_id": line, "reason_code": reason,
                    "planned_flag": int(flag) != 0, "start_utc": start, "end_utc": end,
                    "start_local": utc_to_local(start, tz), "end_local": utc_to_local(end, tz)})
    return out


def downtime_shift_seg(raw, dl, sc):
    """mes.usp_split_downtime_by_shift: strict overlap on LOCAL bounds."""
    by_plant = defaultdict(list)
    for s in sc:
        tz = raw.plant_tz[s["plant_id"]]
        by_plant[s["plant_id"]].append((s, utc_to_local(s["start_utc"], tz), utc_to_local(s["end_utc"], tz)))
    out = []
    for e in dl:
        for s, sl, el in by_plant[e["plant_id"]]:
            if sl < e["end_local"] and el > e["start_local"]:
                out.append({**{k: e[k] for k in ("event_id", "plant_id", "line_id", "reason_code", "planned_flag")},
                            "production_day": s["production_day"], "shift_code": s["shift_code"],
                            "seg_start_local": max(e["start_local"], sl),
                            "seg_end_local": min(e["end_local"], el)})
    return out


def production_local(raw, sc):
    """mes.usp_stg_production_counts: JOIN line/plant/shift_calendar on
    [start_utc, end_utc) by bucket START; production_day = date(local - 6h)."""
    by_plant = defaultdict(list)
    for s in sc:
        by_plant[s["plant_id"]].append(s)
    out = []
    for b in raw.buckets:
        line, sku = (b["line_id"] or "").strip(), (b["sku_id"] or "").strip()
        start, end = _ts(b["bucket_start_utc"]), _ts(b["bucket_end_utc"])
        tot, good = _int(b["total_units"]), _int(b["good_units"])
        if not line or not sku or start is None or end is None or tot is None or good is None:
            continue
        plant = raw.line_plant.get(line)
        if plant is None or plant not in raw.plant_tz:
            continue
        pday = (utc_to_local(start, raw.plant_tz[plant]) - timedelta(hours=6)).date()
        for s in by_plant[plant]:
            if s["start_utc"] <= start < s["end_utc"]:
                out.append({"line_id": line, "plant_id": plant, "sku_id": sku, "production_day": pday,
                            "shift_code": s["shift_code"], "bucket_minutes": datediff_minute(start, end),
                            "total_units": tot, "good_units": good})
    return out


def planned_time(raw, sc, seg):
    """mes.usp_calc_planned_time: shift_calendar JOIN dim.line (by plant)
    LEFT JOIN seg; shift_minutes on UTC bounds, segment minutes on LOCAL bounds."""
    agg = defaultdict(lambda: [None, None])
    for s in seg:
        k = (s["plant_id"], s["line_id"], s["production_day"], s["shift_code"])
        i = 0 if s["planned_flag"] else 1
        agg[k][i] = (agg[k][i] or 0) + datediff_minute(s["seg_start_local"], s["seg_end_local"])
    lines_by_plant = defaultdict(list)
    for line, plant in raw.line_plant.items():
        lines_by_plant[plant].append(line)
    out = {}
    for s in sc:
        for line in lines_by_plant[s["plant_id"]]:
            k = (s["plant_id"], line, s["production_day"], s["shift_code"])
            pdt, udt = agg.get(k, [None, None])
            out[k] = {"shift_minutes": datediff_minute(s["start_utc"], s["end_utc"]),
                      "planned_dt_min": pdt or 0, "unplanned_dt_min": udt or 0}
    return out


def _fit(x, scale, how):
    return x.quantize(Decimal(1).scaleb(-scale), rounding=how)


def metrics(shift, udt, total, good, ideal, mode="tsql"):
    """The four rpt.usp_rpt_oee_shift ratios as Decimal (None = NULL)."""
    with localcontext() as ctx:
        ctx.prec = 120
        div = {"tsql": ROUND_DOWN, "round": ROUND_HALF_UP}.get(mode)
        mul = ROUND_HALF_UP

        def fit(x, s, how):
            return x if mode == "exact" else _fit(x, s, how)

        def r4(x):
            return None if x is None else _fit(x, 4, ROUND_HALF_UP)

        sm, up = Decimal(shift), Decimal(shift - udt)
        tot, gd = Decimal(total), Decimal(good)
        a = fit(up / sm, 15, div) if shift != 0 else None
        p = fit(tot / ideal, 22, div) if ideal != 0 else None
        q = fit(gd / tot, 15, div) if total != 0 else None
        oee = None
        if a is not None and ideal != 0 and total != 0:
            x2 = fit(a * tot, 9, mul)
            x3 = fit(x2 / ideal, 7, div)
            x4 = fit(x3 * gd, 6, mul)
            oee = fit(x4 / tot, 6, div)
        return r4(a), r4(p), r4(q), r4(oee)


def oee_shift(raw_dir, as_of, mode="tsql", stages=None):
    """rows of rpt.oee_shift as dicts (Decimals / None), sorted by key."""
    raw = Raw(raw_dir)
    sc = shift_calendar(raw)
    seg = downtime_shift_seg(raw, downtime_local(raw, as_of), sc)
    pl = production_local(raw, sc)
    pt = planned_time(raw, sc, seg)
    prod = {}
    for b in pl:
        rate = raw.sku_rate.get(b["sku_id"])
        if rate is None:  # JOIN dim.sku
            continue
        k = (b["plant_id"], b["line_id"], b["production_day"], b["shift_code"])
        t = prod.setdefault(k, [0, 0, Decimal(0)])
        t[0] += b["total_units"]
        t[1] += b["good_units"]
        t[2] += rate * b["bucket_minutes"]
    rows = []
    for k in sorted(pt):
        if k[2] < LEGACY_MIN_PRODUCTION_DAY or k not in prod:
            continue
        p = pt[k]
        tot, good, ideal = prod[k]
        a, perf, q, oee = metrics(p["shift_minutes"], p["unplanned_dt_min"], tot, good, ideal, mode)
        rows.append(dict(zip(COLUMNS, (*k, p["shift_minutes"], p["unplanned_dt_min"], a, perf, q, oee))))
    if stages is not None:
        stages.update(shift_calendar=sc, downtime_shift_seg=seg, production_local=pl,
                      planned_time=pt, prod=prod)
    return rows


def bcp_lines(rows):
    """MANIFEST bcp -c -t, rendering: NULL empty, decimals with 4 dp."""
    def cell(v):
        if v is None:
            return ""
        if isinstance(v, Decimal):
            return f"{v:.4f}"
        return str(v)
    return [",".join(cell(r[c]) for c in COLUMNS) for r in rows]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default=str(ROOT / "data" / "raw"))
    ap.add_argument("--as-of-utc", default="2025-11-17 00:00:00")
    ap.add_argument("--mode", default="tsql", choices=("tsql", "round", "exact"))
    ap.add_argument("--compare", default=None)
    a = ap.parse_args(argv)
    rows = oee_shift(a.raw_dir, datetime.strptime(a.as_of_utc, TS), a.mode)
    lines = bcp_lines(rows)
    if not a.compare:
        print(",".join(COLUMNS))
        print("\n".join(lines))
        return 0
    snap = Path(a.compare).read_bytes().decode().split("\r\n")
    body = [x for x in snap[1:] if x]
    want = {tuple(x.split(",")[:4]): x for x in body}
    got = {tuple(x.split(",")[:4]): x for x in lines}
    diff = [k for k in want if k in got and want[k] != got[k]]
    print(f"mode={a.mode} rows oracle={len(got)} snapshot={len(want)} "
          f"missing={len(set(want) - set(got))} extra={len(set(got) - set(want))} value_diffs={len(diff)}")
    for k in diff[:10]:
        print("  want", want[k], "\n  got ", got[k])
    rendered = (",".join(COLUMNS) + "\r\n" + "".join(x + "\r\n" for x in lines)).encode()
    print("byte_identical:", rendered == Path(a.compare).read_bytes())
    return 0 if not diff and set(want) == set(got) else 1


if __name__ == "__main__":
    sys.exit(main())
