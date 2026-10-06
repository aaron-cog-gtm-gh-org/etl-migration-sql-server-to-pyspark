#!/usr/bin/env python3
"""Regenerate legacy_snapshots/ against real SQL Server 2022.

Connects to the docker-compose `mssql` service, creates schemas and
tables, bulk loads data/raw/*.csv, executes the same proc sequence the
ADF PL_Master pipeline runs, then exports every rpt.* table to
legacy_snapshots/<table>.csv (deterministic ORDER BY, stable format).

Requires: pymssql (pip). Usage: .venv/bin/python tools/legacy_run.py
"""
import csv
import os
import sys
from pathlib import Path

import pymssql

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
SNAP = ROOT / "legacy_snapshots"

HOST = os.environ.get("MSSQL_HOST", "localhost")
USER, PASSWORD, DB = "sa", "Loc4lDevOnly!nwh", "MESDB"
AS_OF_UTC = "2026-11-16 00:00:00"   # fixed report cutoff (deterministic)
WINDOW_START, WINDOW_DAYS = "2026-10-19", 28

# Proc sequence in PL_Master dependency order
PROC_SEQUENCE = [
    ("dim.usp_refresh_shift_calendar", f"@StartDate='{WINDOW_START}', @Days={WINDOW_DAYS}"),
    ("mes.usp_stg_downtime_local",     f"@AsOfUtc='{AS_OF_UTC}'"),
    ("mes.usp_split_downtime_by_shift", ""),
    ("rpt.usp_rpt_line_downtime_daily", ""),
    ("mes.usp_stg_production_counts",  ""),
    ("rpt.usp_rpt_daily_production",   ""),
    ("mes.usp_calc_planned_time",      ""),
    ("rpt.usp_rpt_oee_shift",          ""),
    ("mes.usp_stg_quality_samples",    ""),
    ("mes.usp_merge_quality_hold_status", f"@AsOfUtc='{AS_OF_UTC}'"),
    ("rpt.usp_rpt_open_quality_holds", f"@AsOfUtc='{AS_OF_UTC}'"),
    ("mes.usp_allocate_scrap_to_orders", ""),
    ("rpt.usp_rpt_scrap_yield_weekly", ""),
    ("mes.usp_stg_material_movements", ""),
    ("rpt.usp_rpt_material_variance",  ""),
]

RPT_TABLES = [
    "rpt.line_downtime_daily", "rpt.daily_production", "rpt.oee_shift",
    "rpt.open_quality_holds", "rpt.scrap_yield_weekly", "rpt.material_variance",
]

ORDER_BY = {
    "rpt.line_downtime_daily": "plant_id, line_id, production_day, shift_code, reason_category, planned_flag",
    "rpt.daily_production": "plant_id, line_id, production_day, sku_id",
    "rpt.oee_shift": "plant_id, line_id, production_day, shift_code",
    "rpt.open_quality_holds": "lot_id",
    "rpt.scrap_yield_weekly": "plant_id, line_id, iso_year, iso_week",
    "rpt.material_variance": "order_id, material_code",
}

# load order respects FK direction (dim before mes)
LOAD_ORDER = [
    "dim.plant", "dim.line", "dim.shift_pattern", "dim.calendar", "dim.sku",
    "dim.downtime_reason", "dim.material_standard",
    "mes.production_count", "mes.downtime_event", "mes.line_speed_sample",
    "mes.production_order", "mes.scrap_event", "mes.quality_sample",
    "mes.quality_hold_event", "mes.material_movement",
]


def run_script(cur, path: Path):
    text = path.read_text()
    for batch in text.split("\nGO"):
        batch = batch.strip()
        if batch:
            cur.execute(batch)


def main():
    print(f"connecting {USER}@{HOST} ...")
    adm = pymssql.connect(HOST, USER, PASSWORD, "master", autocommit=True)
    cur = adm.cursor()
    cur.execute(f"IF DB_ID('{DB}') IS NULL CREATE DATABASE {DB}")
    adm.close()

    conn = pymssql.connect(HOST, USER, PASSWORD, DB, autocommit=True)
    cur = conn.cursor()

    print("creating schema ...")
    for f in sorted((ROOT / "legacy/sql/schema").glob("*.sql")):
        run_script(cur, f)
    for f in sorted((ROOT / "legacy/sql/functions").glob("*.sql")):
        run_script(cur, f)
    for f in sorted((ROOT / "legacy/sql/procs").glob("*.sql")):
        run_script(cur, f)

    print("bulk loading data/raw ...")
    for table in LOAD_ORDER:
        p = RAW / f"{table}.csv"
        with p.open(newline="") as fh:
            rdr = csv.reader(fh)
            header = next(rdr)
            # %s placeholders; pymssql does not do real bulk — executemany is fine at this volume
            rows = [tuple(None if v == "" else v for v in r) for r in rdr]
        ph = ",".join(["%s"] * len(header))
        cur.executemany(f"INSERT INTO {table} ({','.join(header)}) VALUES ({ph})", rows)
        print(f"  {table}: {len(rows)} rows")

    print("executing PL_Master proc sequence ...")
    for proc, args in PROC_SEQUENCE:
        cur.execute(f"EXEC {proc} {args}" if args else f"EXEC {proc}")
        print(f"  {proc}: ok")

    print("exporting snapshots ...")
    SNAP.mkdir(exist_ok=True)
    for t in RPT_TABLES:
        cur.execute(f"SELECT * FROM {t} ORDER BY {ORDER_BY[t]}")
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        with (SNAP / f"{t}.csv").open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(cols)
            for r in rows:
                w.writerow("" if v is None else v for v in r)
        print(f"  {t}: {len(rows)} rows -> legacy_snapshots/{t}.csv")

    conn.close()
    print("legacy_run complete")


if __name__ == "__main__":
    main()
