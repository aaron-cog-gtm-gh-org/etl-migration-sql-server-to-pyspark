#!/usr/bin/env python3
"""Deterministic synthetic MES seed -> data/raw/*.csv

Regenerated identically with a fixed seed. Emulates the raw feeds that land
in the mes.* staging tables for Kimberly-Clark.

Usage: python tools/seed.py
"""
import csv
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"

SEED = 20251020
START = date(2025, 10, 20)
DAYS = 28  # 2025-10-20 .. 2025-11-16, spans EU DST end 2025-10-26 and US DST end 2025-11-02

# plant_id -> (name, IANA tz for generating UTC feed timestamps, pattern)
# NOTE: dim.plant stores the *Windows* tz name used by SQL Server AT TIME ZONE.
PLANTS = {
    "PLT01": ("Neenah Tissue",      "Central Standard Time",           "America/Chicago",    "3x8"),
    "PLT02": ("Jacksonville Paper", "Eastern Standard Time",           "America/New_York",   "3x8"),
    "PLT03": ("Reno Converting",    "Pacific Standard Time",           "America/Los_Angeles","3x8"),
    "PLT04": ("Monterrey Wipes",    "Central Standard Time (Mexico)",  "America/Mexico_City","3x8"),
    "PLT05": ("Sao Paulo Tissue",   "E. South America Standard Time",  "America/Sao_Paulo",  "2x12"),
    "PLT06": ("Hull Wipes",         "GMT Standard Time",               "Europe/London",      "2x12"),
}

# sku_id, product, pack_size (each per case), ideal units/min on a healthy line
SKUS = [
    ("SKU-TT24", "Bath tissue 2-ply, 24 roll case",   24, 240),
    ("SKU-TT18", "Bath tissue 1-ply, 18 roll case",   18, 210),
    ("SKU-FT30", "Facial tissue cube, 30 box case",   30, 300),
    ("SKU-DP32", "Diaper size 4, 32 ct case",         32, 180),
    ("SKU-DP40", "Diaper size 5, 40 ct case",         40, 190),
    ("SKU-WP36", "Wet wipes tub, 36 ct case",         36, 260),
    ("SKU-WP12", "Wet wipes travel, 12 pk case",      12, 150),
    ("SKU-TT12", "Bath tissue 3-ply, 12 roll case",   12, 200),
]

REASONS = [
    # reason_code, description, category, typically_planned
    ("R-JAM",   "Sheet jam / web break",          "Mechanical", 0),
    ("R-BRG",   "Bearing / drive fault",          "Mechanical", 0),
    ("R-ELE",   "Electrical fault",               "Electrical", 0),
    ("R-CO",    "Changeover / grade change",      "Changeover", 1),
    ("R-MAT",   "Material starvation",            "Material",   0),
    ("R-QLT",   "Quality hold adjustment",        "Quality",    0),
    ("R-PM",    "Preventive maintenance window",  "Planned",    1),
    ("R-SAN",   "Sanitation / clean cycle",       "Planned",    1),
    ("R-OPR",   "Operator assistance",            "Operational",0),
    ("R-OTH",   "Unclassified",                   "Other",      0),
]

MATERIALS = ["PULP-KRAFT-001", "PULP-RCYC-004", "POLY-BACK-11", "ADH-HT-077",
             "PULP-SOFT-009", "SAP-GRAN-022", "INK-FLX-003", "CART-CRG-15"]

DAYS_LIST = [START + timedelta(days=i) for i in range(DAYS)]


def shifts(pattern):
    if pattern == "3x8":
        return [("S1", 6, 14), ("S2", 14, 22), ("S3", 22, 30)]
    return [("D", 6, 18), ("N", 18, 30)]


def local_dt(d, h, m=0, s=0):
    return datetime(d.year, d.month, d.day, 0, 0) + timedelta(hours=h, minutes=m, seconds=s)


def to_utc(iana, naive):
    return naive.replace(tzinfo=ZoneInfo(iana)).astimezone(timezone.utc).replace(tzinfo=None)


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def write_csv(name, header, rows):
    p = RAW / name
    with p.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  {name}: {len(rows)} rows")


def main():
    rng = random.Random(SEED)
    RAW.mkdir(parents=True, exist_ok=True)

    # ---------------- dim.plant ----------------
    write_csv("dim.plant.csv",
              ["plant_id", "plant_name", "tz_name", "shift_pattern"],
              [(pid, name, win_tz, pat) for pid, (name, win_tz, _, pat) in PLANTS.items()])

    # ---------------- dim.line ----------------
    lines = []
    for pid in PLANTS:
        for i in range(1, 5):
            lines.append((f"{pid}-L{i}", pid, f"Line {i}"))
    write_csv("dim.line.csv", ["line_id", "plant_id", "line_name"], lines)

    # ---------------- dim.shift_pattern ----------------
    rows = []
    for pid, (_, _, _, pat) in PLANTS.items():
        for code, h0, h1 in shifts(pat):
            rows.append((pid, code, f"{h0 % 24:02d}:00", f"{h1 % 24:02d}:00",
                         1 if h1 >= 24 else 0))  # wraps midnight
    write_csv("dim.shift_pattern.csv",
              ["plant_id", "shift_code", "local_start", "local_end", "end_next_day"], rows)

    # ---------------- dim.calendar ----------------
    rows = []
    for d in DAYS_LIST:
        iso = d.isocalendar()
        rows.append((d.isoformat(), iso[1], d.isoweekday(), d.strftime("%A")))
    write_csv("dim.calendar.csv",
              ["calendar_date", "iso_week", "day_of_week", "day_name"], rows)

    # ---------------- dim.sku ----------------
    write_csv("dim.sku.csv", ["sku_id", "product_name", "pack_size", "ideal_units_per_min"],
              SKUS)

    # ---------------- dim.downtime_reason ----------------
    write_csv("dim.downtime_reason.csv",
              ["reason_code", "reason_desc", "reason_category", "planned_default"],
              REASONS)

    # line -> sku assignment (primary sku, occasionally secondary)
    line_sku = {}
    for lid, pid, _ in lines:
        sk = rng.choice(SKUS)[0]
        line_sku[lid] = sk

    # ---------------- mes.production_count ----------------
    # 15-min buckets across each production day (local 06:00 -> next 06:00).
    pc_rows = []
    for lid, pid, _ in lines:
        _, win_tz, iana, pat = PLANTS[pid]
        sku_id = line_sku[lid]
        pack = next(s[2] for s in SKUS if s[0] == sku_id)
        ideal = next(s[3] for s in SKUS if s[0] == sku_id)
        for d in DAYS_LIST:
            for b in range(96):
                lstart = local_dt(d, 6) + timedelta(minutes=15 * b)
                lend = lstart + timedelta(minutes=15)
                if rng.random() > 0.95:
                    continue  # idle bucket, no count row
                su = to_utc(iana, lstart)
                eu = to_utc(iana, lend)
                eff = rng.uniform(0.78, 0.99)
                total = int(ideal * 15 * eff)
                # occasionally an explicit zero-total feed row (total=0)
                if rng.random() < 0.006:
                    total = 0
                # a whole dead day on one line -> zero-total report group
                if lid == "PLT06-L4" and d == DAYS_LIST[15]:
                    total = 0
                # scrap/reject ~0.5-3% of units so shift quality lands ~0.97-0.995
                good = total - (int(total * rng.uniform(0.005, 0.03))
                                if total else 0)
                pc_rows.append((lid, fmt(su), fmt(eu), sku_id, total, good))
    write_csv("mes.production_count.csv",
              ["line_id", "bucket_start_utc", "bucket_end_utc", "sku_id",
               "total_units", "good_units"], pc_rows)

    # ---------------- mes.downtime_event ----------------
    de_rows = []
    eid = 100000

    def add_dt(lid, pid, start_utc, end_utc, reason, planned):
        nonlocal eid
        eid += 1
        de_rows.append((eid, lid, fmt(start_utc), fmt(end_utc) if end_utc else "",
                        reason, planned))

    for lid, pid, _ in lines:
        _, win_tz, iana, pat = PLANTS[pid]
        # ambient random events, ~1 every other day
        for d in DAYS_LIST:
            if rng.random() < 0.55:
                h = rng.uniform(6.5, 29.0)
                dur = rng.randint(5, 170)
                ls = local_dt(d, 0) + timedelta(hours=h)
                add_dt(lid, pid, to_utc(iana, ls),
                       to_utc(iana, ls + timedelta(minutes=dur)),
                       rng.choice(REASONS)[0], 0)
        # edge: event crossing a shift boundary (local 13:30 -> 14:45)
        d = DAYS_LIST[3]
        add_dt(lid, pid, to_utc(iana, local_dt(d, 13, 30)),
               to_utc(iana, local_dt(d, 14, 45)), "R-JAM", 0)
        # edge: event crossing local midnight (23:40 -> 00:35)
        d = DAYS_LIST[5]
        add_dt(lid, pid, to_utc(iana, local_dt(d, 23, 40)),
               to_utc(iana, local_dt(d, 0) + timedelta(hours=24, minutes=35)),
               "R-MAT", 0)
        # edge: event with seconds on the boundary (10:00:59 -> 10:01:00)
        d = DAYS_LIST[7]
        add_dt(lid, pid, to_utc(iana, local_dt(d, 10, 0, 59)),
               to_utc(iana, local_dt(d, 10, 1, 0)), "R-OPR", 0)

    # edge: events crossing the DST fall-back transition
    dst_cases = [
        # plant, transition day, local start, local end (end may be ambiguous)
        ("PLT01", date(2025, 11, 2), (0, 30), (2, 30)),   # falls back at 02:00
        ("PLT02", date(2025, 11, 2), (0, 45), (2, 15)),
        ("PLT03", date(2025, 11, 2), (0, 15), (3, 5)),
        ("PLT06", date(2025, 10, 26), (0, 30), (2, 30)),  # UK falls back 02:00
    ]
    for pid, d, (h0, m0), (h1, m1) in dst_cases:
        _, _, iana, _ = PLANTS[pid]
        lid = f"{pid}-L1"
        # start in the pre-transition fold; build UTC by hand so the event
        # genuinely spans the instant clocks go back
        ls = local_dt(d, h0, m0)
        le = local_dt(d, h1, m1)
        add_dt(lid, pid, to_utc(iana, ls), to_utc(iana, le), "R-BRG", 0)

    # edge: open events (end NULL) still running as of the report cutoff
    add_dt("PLT01-L2", "PLT01", to_utc("America/Chicago", local_dt(DAYS_LIST[-1], 20, 15)),
           None, "R-JAM", 0)
    add_dt("PLT05-L3", "PLT05", to_utc("America/Sao_Paulo", local_dt(DAYS_LIST[-1], 9, 5)),
           None, "R-ELE", 0)
    # planned PM window on a couple of lines
    add_dt("PLT02-L1", "PLT02", to_utc("America/New_York", local_dt(DAYS_LIST[10], 6, 0)),
           to_utc("America/New_York", local_dt(DAYS_LIST[10], 7, 30)), "R-PM", 1)
    add_dt("PLT04-L2", "PLT04", to_utc("America/Mexico_City", local_dt(DAYS_LIST[12], 14, 0)),
           to_utc("America/Mexico_City", local_dt(DAYS_LIST[12], 15, 0)), "R-SAN", 1)

    write_csv("mes.downtime_event.csv",
              ["event_id", "line_id", "start_utc", "end_utc", "reason_code", "planned_flag"],
              de_rows)

    # ---------------- mes.line_speed_sample ----------------
    ls_rows = []
    sid = 200000
    for lid, pid, _ in lines:
        _, _, iana, _ = PLANTS[pid]
        for d in DAYS_LIST[::3]:
            for k in range(3):
                ls = local_dt(d, 8) + timedelta(hours=k * 6, minutes=rng.randint(0, 59))
                sid += 1
                ls_rows.append((sid, lid, fmt(to_utc(iana, ls)),
                                round(rng.uniform(120, 320), 1)))
    write_csv("mes.line_speed_sample.csv",
              ["sample_id", "line_id", "sample_ts_utc", "units_per_min"], ls_rows)

    # ---------------- mes.production_order ----------------
    po_rows = []
    oid = 300000
    for lid, pid, _ in lines:
        _, _, iana, _ = PLANTS[pid]
        sku_id = line_sku[lid]
        for i, d in enumerate(DAYS_LIST[::4]):
            oid += 1
            ls = local_dt(d, 6) + timedelta(hours=(i % 3) * 2)
            le = ls + timedelta(hours=10)  # overlaps shift boundary
            po_rows.append((f"ORD-{oid}", lid, pid, sku_id,
                            fmt(to_utc(iana, ls)), fmt(to_utc(iana, le)),
                            rng.randint(8000, 40000)))
            # a second order overlapping the first on some days, so scrap
            # events during the overlap have multiple candidate orders
            if rng.random() < 0.55:
                oid += 1
                ls2 = ls + timedelta(hours=5)
                le2 = ls2 + timedelta(hours=7)
                po_rows.append((f"ORD-{oid}", lid, pid, sku_id,
                                fmt(to_utc(iana, ls2)), fmt(to_utc(iana, le2)),
                                rng.randint(8000, 40000)))
    write_csv("mes.production_order.csv",
              ["order_id", "line_id", "plant_id", "sku_id",
               "sched_start_utc", "sched_end_utc", "planned_qty"], po_rows)

    # ---------------- mes.scrap_event ----------------
    sc_rows = []
    cid = 400000
    for lid, pid, _ in lines:
        _, _, iana, _ = PLANTS[pid]
        for d in DAYS_LIST[::2]:
            for k in range(rng.randint(1, 3)):
                cid += 1
                ls = local_dt(d, 7) + timedelta(hours=rng.randint(0, 14),
                                                minutes=rng.randint(0, 59))
                sc_rows.append((cid, lid, fmt(to_utc(iana, ls)),
                                rng.randint(5, 400),
                                rng.choice(["edge trim", "startup waste", "reject",
                                            "splice", "contamination"])))
    write_csv("mes.scrap_event.csv",
              ["scrap_id", "line_id", "scrap_ts_utc", "qty_units", "scrap_type"], sc_rows)

    # ---------------- mes.quality_sample / quality_hold_event ----------------
    qs_rows = []
    qh_rows = []
    qid = 500000
    hid = 600000
    lot_n = 0
    for lid, pid, _ in lines:
        _, _, iana, _ = PLANTS[pid]
        for d in DAYS_LIST[::4]:
            lot_n += 1
            lot = f"LOT-{lid}-{d.strftime('%Y%m%d')}"
            base = local_dt(d, 9) + timedelta(hours=rng.randint(0, 8))
            for s_i in range(3):
                qid += 1
                qs_rows.append((qid, lot, lid,
                                fmt(to_utc(iana, base + timedelta(hours=s_i))),
                                rng.choice(["PASS", "PASS", "PASS", "FAIL"]),
                                rng.choice(["N"] * 6 + ["Y"])))
            # hold lifecycle: ON_HOLD then RELEASED (mostly), some still ON_HOLD
            hid += 1
            t0 = base + timedelta(hours=1)
            qh_rows.append((hid, lot, "ON_HOLD", fmt(to_utc(iana, t0))))
            if rng.random() < 0.8:
                hid += 1
                qh_rows.append((hid, lot, "RELEASED",
                                fmt(to_utc(iana, t0 + timedelta(hours=rng.randint(2, 9))))))
        # edge: duplicate status timestamps -> latest decided by event_id tiebreak
        for d in DAYS_LIST[1::9]:
            lot_n += 1
            lot = f"LOT-{lid}-{d.strftime('%Y%m%d')}-T"
            ts = fmt(to_utc(iana, local_dt(d, 11, 30)))
            hid += 1
            qh_rows.append((hid, lot, "ON_HOLD", ts))
            hid += 1
            qh_rows.append((hid, lot, "RELEASED", ts))  # same ts, higher id wins
    write_csv("mes.quality_sample.csv",
              ["sample_id", "lot_id", "line_id", "sample_ts_utc", "result", "hold_flag"],
              qs_rows)
    write_csv("mes.quality_hold_event.csv",
              ["event_id", "lot_id", "status", "status_ts_utc"], qh_rows)

    # ---------------- mes.material_movement + dim.material_standard ----
    # Each order has a BOM; movements issue most BOM materials (with the
    # odd substitution), standards cover the BOM plus the occasional
    # never-issued line. CHAR(18) on the SQL side: some feeds pad codes
    # with trailing spaces.
    ORDER_BOM = {r[0]: rng.sample(MATERIALS, rng.randint(3, 5)) for r in po_rows}
    mm_rows = []
    ms_rows = []
    mid = 700000
    for oidv, bom in ORDER_BOM.items():
        for m in bom:
            if rng.random() < 0.88:  # issued
                for mv in (("ISSUE", "ISSUE", "RETURN") if rng.random() < 0.3
                           else ("ISSUE", "ISSUE")):
                    mid += 1
                    pad = " " * rng.randint(0, 4) if rng.random() < 0.4 else ""
                    mm_rows.append((mid, oidv, (m + pad)[:18],
                                    mv, round(rng.uniform(50, 900), 2),
                                    fmt(datetime(2025, 10, 20) + timedelta(
                                        days=rng.randint(0, 27),
                                        hours=rng.randint(6, 20)))))
            ms_rows.append((oidv, m, round(rng.uniform(0.5, 5.0), 3)))
        # occasional substitution: issued but not on the BOM
        if rng.random() < 0.08:
            sub = rng.choice([m for m in MATERIALS if m not in bom])
            mid += 1
            mm_rows.append((mid, oidv, sub, "ISSUE",
                            round(rng.uniform(50, 400), 2),
                            fmt(datetime(2025, 10, 20) + timedelta(
                                days=rng.randint(0, 27),
                                hours=rng.randint(6, 20)))))
    write_csv("mes.material_movement.csv",
              ["movement_id", "order_id", "material_code", "movement_type",
               "quantity", "ts_utc"], mm_rows)
    write_csv("dim.material_standard.csv",
              ["order_id", "material_code", "std_qty_per_unit"], ms_rows)

    print(f"seed complete -> {RAW}")


if __name__ == "__main__":
    main()
