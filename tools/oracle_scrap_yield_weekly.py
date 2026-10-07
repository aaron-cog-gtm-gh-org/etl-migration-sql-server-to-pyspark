"""Pure-Python oracle of the legacy PL_Scrap_Yield procs (no Spark).

Independent of mfg_lake.jobs.scrap_yield_weekly; used by
tests/test_scrap_yield_weekly.py and tools/fuzz_scrap_yield_weekly.py.

  cursor_alloc()   literal port of the mes.usp_allocate_scrap_to_orders
                   cursor (row by row, ORDER BY scrap_id, #ov temp table,
                   floor insert, remainder UPDATE via TOP 1 ORDER BY
                   ov_secs DESC, order_id) -> stg.scrap_alloc rows
  report()         rpt.usp_rpt_scrap_yield_weekly over stg.production_local
                   (rebuilt from mes.production_count like
                   mes.usp_stg_production_counts) and mes.scrap_event

Inputs are csv.DictReader-style rows (strings; "" = NULL).
`@ts - 7200` / `@ts + 7200` in the proc is read as +-7200 SECONDS (the
DATEADD(SECOND, +-7200, @ts) bounds of the same WHERE clause); see
docs/migration/scrap_yield_weekly.md.
"""
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fuzz_daily_production import Dims, oracle_shift_calendar  # noqa: E402,F401

TS = "%Y-%m-%d %H:%M:%S"
INT_MAX = 2**31 - 1
WINDOW = timedelta(seconds=7200)
# NOT NULL columns per legacy/sql/schema/mes.sql; rows with a NULL are dropped
PC_NOT_NULL = ("line_id", "bucket_start_utc", "bucket_end_utc", "sku_id",
               "total_units", "good_units")
SE_NOT_NULL = ("scrap_id", "line_id", "scrap_ts_utc", "qty_units", "scrap_type")
PO_NOT_NULL = ("order_id", "line_id", "plant_id", "sku_id", "sched_start_utc",
               "sched_end_utc", "planned_qty")


def _null(r, cols):
    return any(r.get(c) in ("", None) for c in cols)


def _ts(s):
    return datetime.strptime(s, TS)


def _int(v):
    """SQL Server INT: out-of-range raises (Arithmetic overflow)."""
    if not -INT_MAX - 1 <= v <= INT_MAX:
        raise ArithmeticError(f"Arithmetic overflow converting {v} to INT")
    return v


def iso_year_week(d):
    """YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, d), d)), DATEPART(ISO_WEEK, d)."""
    if isinstance(d, datetime):
        d = d.date()
    wk = d.isocalendar()[1]
    return (d + timedelta(days=26 - wk)).year, wk


# ------------------------------------------------ mes.usp_allocate_scrap_to_orders
def cursor_alloc(scraps, orders):
    """Literal cursor port -> list of (scrap_id, order_id, qty_units)."""
    orders = [o for o in orders if not _null(o, PO_NOT_NULL)]
    alloc = []  # stg.scrap_alloc, insertion order
    cur = sorted((r for r in scraps if not _null(r, SE_NOT_NULL)),
                 key=lambda r: int(r["scrap_id"]))
    for r in cur:
        scrap_id, line_id = int(r["scrap_id"]), r["line_id"].strip()
        ts, qty = _ts(r["scrap_ts_utc"]), int(r["qty_units"])
        ov = []  # #ov
        for o in orders:
            s, e = _ts(o["sched_start_utc"]), _ts(o["sched_end_utc"])
            if (o["line_id"].strip() == line_id and s < ts + WINDOW and e > ts - WINDOW
                    and s <= ts <= e):
                lo = s if s > ts - WINDOW else ts - WINDOW
                hi = e if e < ts + WINDOW else ts + WINDOW
                ov.append((o["order_id"].strip(), int((hi - lo).total_seconds())))
        if ov:
            tot = _int(sum(v for _, v in ov))
            if tot == 0:
                raise ZeroDivisionError("Divide by zero error encountered.")
            for oid, v in ov:
                num = _int(qty * v)  # INT * INT
                # numeric(13,1) / numeric(10,0) -> scale 12, then CAST AS INT truncates
                q = (Decimal(num) / Decimal(tot)).quantize(Decimal("1e-12"), ROUND_HALF_UP)
                alloc.append([scrap_id, oid, int(q.to_integral_value(ROUND_DOWN))])
            big = sorted(ov, key=lambda x: (-x[1], x[0]))[0][0]
            rem = qty - sum(a[2] for a in alloc if a[0] == scrap_id)
            for a in alloc:
                if a[0] == scrap_id and a[1] == big:
                    a[2] = _int(a[2] + rem)
        else:
            alloc.append([scrap_id, "UNALLOCATED", qty])
    return [tuple(a) for a in alloc]


# ------------------------------------------------ rpt.usp_rpt_scrap_yield_weekly
def good_weekly(d: Dims, counts):
    """stg.production_local (no dim.sku join) -> SUM(good_units) by ISO week
    of production_day = date(local(bucket_start) - 6h)."""
    sc = oracle_shift_calendar(d)
    agg = defaultdict(int)
    for r in counts:
        if _null(r, PC_NOT_NULL):
            continue
        plant = d.lines.get(r["line_id"].strip())
        if plant is None:
            continue
        bs = _ts(r["bucket_start_utc"])
        local = bs.replace(tzinfo=timezone.utc).astimezone(ZoneInfo(d.tz[plant])).replace(tzinfo=None)
        n = sum(1 for s, e, _, _ in sc[plant] if s <= bs < e)
        if n:
            agg[(plant.strip(), r["line_id"].strip(), *iso_year_week(local - timedelta(hours=6)))] \
                += n * int(r["good_units"])
    return dict(agg)


def scrap_weekly(d: Dims, scraps):
    """mes.scrap_event JOIN dim.line, ISO week of the UTC timestamp."""
    agg = defaultdict(int)
    for r in scraps:
        if _null(r, SE_NOT_NULL):
            continue
        plant = d.lines.get(r["line_id"].strip())
        if plant is None:
            continue
        agg[(plant.strip(), r["line_id"].strip(), *iso_year_week(_ts(r["scrap_ts_utc"])))] \
            += int(r["qty_units"])
    return dict(agg)


def scrap_pct(good, scrap):
    """ROUND(CAST(scrap AS DECIMAL(18,4)) / NULLIF(good + scrap, 0) * 100, 2)."""
    den = _int(good + scrap)
    if den == 0:
        return None
    return (Decimal(scrap) / Decimal(den) * 100).quantize(Decimal("0.01"), ROUND_HALF_UP)


def report(d: Dims, counts, scraps):
    """{(plant_id, line_id, iso_year, iso_week): (good_units, scrap_units, scrap_pct)}
    good LEFT JOIN scrap: scrap weeks with no production row are dropped."""
    good, scrap = good_weekly(d, counts), scrap_weekly(d, scraps)
    out = {}
    for k, g in good.items():
        s = scrap.get(k, 0)
        out[k] = (_int(g), _int(s), scrap_pct(g, s))
    return out
