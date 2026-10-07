#!/usr/bin/env python3
"""Fuzz and reconcile the KAN-7 OEE shift migration.

    python tools/fuzz_oee_shift.py [--n 25] [--seed 20251117]

Each variant mutates copied raw CSV feeds, runs the OEE job in an isolated
namespace, and checks the result against the independent pure-Python oracle.
The canonical seed is also reconciled, rendered in snapshot BCP format, and
tested for determinism, cutoff behaviour, and a corrupted-value negative
control. The structured log is consumed by tools/validation_report.py.
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
from mfg_lake.common.spark import get_spark  # noqa: E402
from mfg_lake.common.tz import windows_to_iana  # noqa: E402
import oracle_oee_shift as oracle  # noqa: E402

RAW = ROOT / "data" / "raw"
SNAPSHOT = ROOT / "legacy_snapshots" / "rpt.oee_shift.csv"
REPORT = "oee_shift"
CANON_AS_OF = "2025-11-17 00:00:00"
EARLY_AS_OF = "2025-11-10 00:00:00"
KEYS = ("plant_id", "line_id", "production_day", "shift_code")
COLUMNS = oracle.COLUMNS
METRICS = ("availability", "performance", "quality", "oee")
FLOOR = date(2025, 10, 20)
TS = "%Y-%m-%d %H:%M:%S"

EXPECTED_SCHEMA = {
    "plant_id": "string",
    "line_id": "string",
    "production_day": "date32[day]",
    "shift_code": "string",
    "planned_min": "int32",
    "unplanned_dt_min": "int32",
    "availability": "decimal128(9, 4)",
    "performance": "decimal128(9, 4)",
    "quality": "decimal128(9, 4)",
    "oee": "decimal128(9, 4)",
}

REPORT_META = {
    "ticket": "KAN-7",
    "pipeline": "PL_OEE",
    "fuzz_input": "mes.production_count + mes.downtime_event + dim.calendar + dim.sku",
    "bcp_note": "ORDER BY key, NULL = empty, decimals with 4 dp",
    "oracle_note_html": (
        "<code>row_by_row_equality</code>, metric checks, and planned-time "
        "checks compare every output against <code>tools/oracle_oee_shift.py</code>, "
        "an independent pure-Python implementation of the legacy data flow."
    ),
    "as_of_header": "as_of_utc (applied through imported downtime stage)",
    "earlier_cutoff_label": (
        "earlier cutoff {as_of_utc} changes OEE through the imported KAN-8 "
        "downtime stage; reconcile FAIL is expected"
    ),
    "earlier_cutoff_detail": (
        "changed_rows={changed_vs_canonical}, reconcile exit "
        "{reconcile_exit_code}; derived from the legacy data flow"
    ),
    "parity_notes_html": [
        "<b>AsOfUtc: discrepancy with the playbook table.</b> Neither OEE proc receives AsOfUtc, and the job adds no cutoff filter. <code>as_of_utc</code> goes only into the imported KAN-8 <code>stage_downtime_local</code> (<code>start_utc &lt; AsOfUtc</code>, open events capped), which feeds <code>stg.downtime_shift_seg</code> → <code>mes.usp_calc_planned_time</code>. An earlier cutoff therefore changes OEE (2025-11-10: 92 values; 2025-11-16 12:00: 9; 2025-11-20: 7; 2025-11-17 00:00:01: 0), and the expected earlier-cutoff outcome is reconcile FAIL. The playbook's AsOfUtc table lists PL_OEE as IDENTICAL, which is wrong for this pipeline.",
        "<b>Ticket behaviour not met, by design.</b> KAN-7 says \"planned minutes = shift length minus PLANNED downtime\". Legacy computes <code>planned_dt_min</code> but never uses it: <code>planned_min = shift_minutes</code> (DATEDIFF on UTC bounds; 540/780 on fall-back nights), and availability = (shift − unplanned) / shift. The extract agrees: PLT02-L1 2025-10-30 S1 has 90 planned downtime minutes, <code>planned_min=480</code>, <code>availability=1.0000</code>.",
        "<b>Rounding.</b> The extract matches each metric's exact ratio rounded once to 4 dp, half away from zero, with OEE = (s−u)·G / (s·I) and a NULLIF guard on total units. A chain of Spark decimal divisions gives 4 diffs, and multiplying rounded components also differs (PLT03-L1 2025-10-23 S3: 0.8655 vs 0.8654). The job uses exact DECIMAL(38,0) integer arithmetic.",
        "<b>Downtime minutes</b> use DATEDIFF(MINUTE) on local segment bounds via KAN-8's <code>datediff_minute</code>. The hard-coded <code>production_day &gt;= '2025-10-20'</code> filter is kept.",
        "<b>Canonical seed edge cases.</b> PLT06-L4 2025-11-04 (zero-total day) gives NULL quality and oee and performance 0.0000. <code>SKU-XX99</code> buckets are dropped by the <code>dim.sku</code> inner join. Spring-forward days, boundary straddles, duplicates, unknown keys, open events and pre-floor rows are covered by the unit tests and the fuzz variants.",
        "<b>No seed tuning was needed.</b> The existing seed reproduces the extract byte-for-byte. The seed was not fitted to the snapshot.",
        "<b>Line terminator.</b> MANIFEST shows <code>bcp -r\\n</code>, but the committed extract uses CRLF. The byte-compare renders CRLF.",
        "<b>NULLs in NOT NULL source columns</b> (impossible in legacy) are dropped and counted by the imported stages. <b>INT / DECIMAL overflow</b> raises, like SQL Server.",
        "<b>Ambiguous / gap local times</b> follow java.time / zoneinfo fold=0 (the documented AT TIME ZONE behaviour). This was not verified against a live SQL Server.",
        "The ADF JSON in <code>lakehouse/adf/</code> was reviewed, not deployed. Nothing here ran on ADF or Databricks.",
    ],
}


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return reader.fieldnames, list(reader)


def write_csv(path, header, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def parse_ts(value):
    return datetime.strptime(value, TS)


def local_to_utc(local_dt, iana):
    return local_dt.replace(tzinfo=ZoneInfo(iana), fold=0).astimezone(
        timezone.utc
    ).replace(tzinfo=None)


def stamp(value):
    return value.strftime(TS)


class RawDims:
    def __init__(self, raw_dir):
        self.raw_dir = Path(raw_dir)
        _, plants = read_csv(self.raw_dir / "dim.plant.csv")
        _, lines = read_csv(self.raw_dir / "dim.line.csv")
        _, patterns = read_csv(self.raw_dir / "dim.shift_pattern.csv")
        self.tz = {
            row["plant_id"].strip(): windows_to_iana(row["tz_name"])
            for row in plants
        }
        self.line_plant = {
            row["line_id"].strip(): row["plant_id"].strip() for row in lines
        }
        self.patterns = patterns
        self.central_plant = next(
            plant for plant, zone in self.tz.items() if zone == "America/Chicago"
        )
        self.central_line = next(
            line for line, plant in self.line_plant.items()
            if plant == self.central_plant
        )
        _, calendar = read_csv(self.raw_dir / "dim.calendar.csv")
        self.calendar_days = {date.fromisoformat(row["calendar_date"]) for row in calendar}


def add_calendar_days(raw_dir, days):
    path = Path(raw_dir) / "dim.calendar.csv"
    header, rows = read_csv(path)
    present = {date.fromisoformat(row["calendar_date"]) for row in rows}
    for day in sorted(set(days) - present):
        rows.append({
            "calendar_date": day.isoformat(),
            "iso_week": str(day.isocalendar().week),
            "day_of_week": str(day.isoweekday()),
            "day_name": day.strftime("%A"),
        })
    rows.sort(key=lambda row: row["calendar_date"])
    write_csv(path, header, rows)


def append_sku_zero_rate(raw_dir):
    path = Path(raw_dir) / "dim.sku.csv"
    header, rows = read_csv(path)
    if any(row["sku_id"] == "SKU-ZERO-RATE" for row in rows):
        return
    template = dict(rows[0])
    template.update({
        "sku_id": "SKU-ZERO-RATE",
        "product_name": "Fuzz zero ideal rate",
        "ideal_units_per_min": "0.00",
    })
    rows.append(template)
    write_csv(path, header, rows)


def add_bucket(raw_dir, line_id, sku_id, start, end, total, good, bucket_id=None):
    path = Path(raw_dir) / "mes.production_count.csv"
    header, rows = read_csv(path)
    template = dict(rows[0])
    next_id = max(int(row["bucket_id"]) for row in rows) + 1
    template.update({
        "bucket_id": str(bucket_id if bucket_id is not None else next_id),
        "line_id": line_id,
        "sku_id": sku_id,
        "bucket_start_utc": stamp(start),
        "bucket_end_utc": stamp(end),
        "total_units": str(total),
        "good_units": str(good),
    })
    rows.append(template)
    write_csv(path, header, rows)
    return template


def add_event(raw_dir, line_id, start, end, reason, planned, event_id=None):
    path = Path(raw_dir) / "mes.downtime_event.csv"
    header, rows = read_csv(path)
    next_id = max(int(row["event_id"]) for row in rows) + 1
    event = {
        "event_id": str(event_id if event_id is not None else next_id),
        "line_id": line_id,
        "start_utc": stamp(start),
        "end_utc": "" if end is None else stamp(end),
        "reason_code": reason,
        "planned_flag": str(planned),
    }
    rows.append(event)
    write_csv(path, header, rows)
    return event


def make_variant(raw_dir, variant_index, rng):
    for source in RAW.glob("*.csv"):
        shutil.copy2(source, raw_dir / source.name)

    spring = variant_index % 4 == 3
    spring_dates = {
        date(2026, 3, 8) + timedelta(days=offset) for offset in range(-6, 30)
    }
    add_calendar_days(raw_dir, {date(2025, 10, 19)} | (spring_dates if spring else set()))
    append_sku_zero_rate(raw_dir)
    dims = RawDims(raw_dir)
    central_line = dims.central_line

    # Random cutoff values remain inside a known calendar window so the open
    # event below contributes a capped segment in some shift.
    if spring:
        cutoff_day = date(2026, 3, 12) + timedelta(days=rng.randrange(20))
    else:
        cutoff_day = date(2025, 10, 25) + timedelta(days=rng.randrange(22))
    as_of = datetime.combine(cutoff_day, datetime.min.time()) + timedelta(
        hours=rng.randrange(6, 24), minutes=rng.choice((0, 17, 41))
    )
    as_of_string = stamp(as_of)
    features = {
        "dst_events": 0,
        "dst_buckets": 0,
        "shift_boundary_events": 0,
        "straddling_buckets": 0,
        "open_events": 0,
        "zero_unit_buckets": 0,
        "zero_rate_buckets": 0,
        "duplicated_buckets": 0,
        "duplicated_events": 0,
        "unknown_sku_buckets": 0,
        "unknown_line_buckets": 0,
        "pre_floor_buckets": 0,
    }

    # These events cross the DST transitions while remaining inside the
    # corresponding night shift; local DATEDIFF remains nonnegative.
    if not spring:
        add_event(
            raw_dir, central_line, datetime(2025, 11, 2, 6, 30),
            datetime(2025, 11, 2, 8, 17), "R-CO", 0,
        )
        add_bucket(
            raw_dir, central_line, "SKU-TT18", datetime(2025, 11, 2, 6, 30),
            datetime(2025, 11, 2, 8, 17), 120, 117,
        )
        features["dst_events"] += 1
        features["dst_buckets"] += 1
    else:
        # Cross the spring gap from 01:00 CST to 04:17 CDT on March 8.
        add_event(
            raw_dir, central_line, datetime(2026, 3, 8, 7),
            datetime(2026, 3, 8, 9, 17), "R-CO", 0,
        )
        add_bucket(
            raw_dir, central_line, "SKU-TT18", datetime(2026, 3, 8, 7),
            datetime(2026, 3, 8, 9, 17), 120, 117,
        )
        features["dst_events"] += 1
        features["dst_buckets"] += 1

    # Central S1 starts at 06:00 local (11:00 UTC in October). Both the event
    # and bucket straddle that boundary; production is assigned by bucket start.
    boundary = datetime(2025, 10, 23, 11)
    boundary_event = add_event(
        raw_dir, central_line, boundary - timedelta(minutes=1),
        boundary + timedelta(minutes=1), "R-ME", 1,
    )
    boundary_bucket = add_bucket(
        raw_dir, central_line, "SKU-TT24", boundary,
        boundary + timedelta(hours=8, minutes=5), 1000, 985,
    )
    features["shift_boundary_events"] += 1
    features["straddling_buckets"] += 1

    # Open event is cut off by the per-variant AsOfUtc.
    add_event(
        raw_dir, central_line, as_of - timedelta(minutes=45), None, "R-CO", 0,
    )
    features["open_events"] += 1
    # Verify the strict start_utc < AsOfUtc gate with a second event at cutoff.
    add_event(raw_dir, central_line, as_of, as_of + timedelta(minutes=20), "R-CO", 0)

    _, buckets = read_csv(raw_dir / "mes.production_count.csv")
    source = dict(buckets[rng.randrange(len(buckets))])
    zero_start = datetime(2025, 10, 27, 11)
    add_bucket(
        raw_dir, central_line, source["sku_id"], zero_start,
        zero_start + timedelta(minutes=15), 0, 0,
    )
    features["zero_unit_buckets"] += 1

    zero_rate_start = datetime(2025, 10, 28, 11)
    add_bucket(
        raw_dir, central_line, "SKU-ZERO-RATE", zero_rate_start,
        zero_rate_start + timedelta(minutes=15), 40, 39,
    )
    features["zero_rate_buckets"] += 1

    # Duplicate input rows are deliberately retained as legacy SUM behaviour.
    duplicate_bucket = dict(boundary_bucket)
    with (raw_dir / "mes.production_count.csv").open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=duplicate_bucket.keys(), lineterminator="\n")
        writer.writerow(duplicate_bucket)
    features["duplicated_buckets"] += 1
    duplicate_event = dict(boundary_event)
    with (raw_dir / "mes.downtime_event.csv").open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=duplicate_event.keys(), lineterminator="\n")
        writer.writerow(duplicate_event)
    features["duplicated_events"] += 1

    # Unknown SKU/line are dropped by the imported production stage / SKU join.
    unknown_sku = dict(source)
    unknown_sku["bucket_id"] = str(max(int(row["bucket_id"]) for row in buckets) + 5)
    unknown_sku["sku_id"] = "SKU-UNKNOWN"
    with (raw_dir / "mes.production_count.csv").open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=unknown_sku.keys(), lineterminator="\n")
        writer.writerow(unknown_sku)
    features["unknown_sku_buckets"] += 1

    unknown_line = dict(source)
    unknown_line["bucket_id"] = str(int(unknown_sku["bucket_id"]) + 1)
    unknown_line["line_id"] = "ZZZ-L9"
    with (raw_dir / "mes.production_count.csv").open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=unknown_line.keys(), lineterminator="\n")
        writer.writerow(unknown_line)
    features["unknown_line_buckets"] += 1

    before_floor_start = datetime(2025, 10, 19, 12)
    add_bucket(
        raw_dir, central_line, "SKU-TT18", before_floor_start,
        before_floor_start + timedelta(minutes=15), 10, 9,
    )
    features["pre_floor_buckets"] += 1

    return as_of_string, features, "spring-2026" if spring else "canonical-2025"


def date_value(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime().date()
    return date.fromisoformat(str(value)[:10])


def normalized_value(column, value):
    if value is None or pd.isna(value):
        return None
    if column == "production_day":
        return date_value(value)
    if column in METRICS:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    if column in ("planned_min", "unplanned_dt_min"):
        return int(value)
    return str(value)


def records_from_parquet(namespace):
    path = curated_dir(REPORT, namespace)
    table = pq.read_table(sorted(path.glob("*.parquet")))
    frame = table.to_pandas()
    rows = []
    for record in frame.to_dict("records"):
        rows.append({
            column: normalized_value(column, record[column]) for column in COLUMNS
        })
    return rows, table.schema, frame


def key_of(row):
    return (
        str(row["plant_id"]),
        str(row["line_id"]),
        date_value(row["production_day"]),
        str(row["shift_code"]),
    )


def row_map(rows):
    return {key_of(row): row for row in rows}


def schema_contract(schema):
    actual = {field.name: str(field.type) for field in schema}
    expected = [EXPECTED_SCHEMA[name] for name in COLUMNS]
    actual_order = [field.name for field in schema]
    ok = actual_order == list(COLUMNS) and [actual.get(name) for name in COLUMNS] == expected
    return {
        "pass": ok,
        "detail": (
            f"columns={actual_order}; types={[actual.get(name) for name in actual_order]}"
            f"; expected columns={list(COLUMNS)}; expected types={expected}"
        ),
    }


def invariant_checks(rows, oracle_rows, stages):
    expected = row_map(oracle_rows)
    actual_keys = [key_of(row) for row in rows]
    actual = row_map(rows)
    duplicate_count = len(actual_keys) - len(set(actual_keys))
    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)
    mismatches = [
        key for key in set(expected) & set(actual)
        if any(actual[key][column] != expected[key][column] for column in COLUMNS)
    ]
    row_equal = not missing and not extra and not mismatches

    planned = stages["planned_time"]
    production = stages["prod"]
    planned_errors = []
    metric_errors = []
    negative_unplanned = []
    floor_errors = []
    subset_errors = []
    for row in rows:
        key = key_of(row)
        pt = planned.get(key)
        prod = production.get(key)
        if pt is None or row["planned_min"] != pt["shift_minutes"]:
            planned_errors.append(key)
        if row["unplanned_dt_min"] < 0:
            negative_unplanned.append(key)
        if row["production_day"] < FLOOR:
            floor_errors.append(key)
        if pt is None or prod is None:
            subset_errors.append(key)
        if pt is not None and prod is not None:
            wanted = oracle.metrics(
                pt["shift_minutes"], pt["unplanned_dt_min"],
                prod[0], prod[1], prod[2], mode="exact",
            )
            if any(row[name] != value for name, value in zip(METRICS, wanted)):
                metric_errors.append(key)

    return {
        "row_by_row_equality": {
            "pass": row_equal,
            "detail": (
                f"missing={len(missing)}, extra={len(extra)}, value_mismatches={len(mismatches)}"
            ),
        },
        "unique_keys": {
            "pass": duplicate_count == 0,
            "detail": f"rows={len(rows)}, duplicate_keys={duplicate_count}",
        },
        "planned_min_matches_utc_shift": {
            "pass": not planned_errors,
            "detail": f"mismatches={len(planned_errors)}",
        },
        "unplanned_minutes_nonnegative": {
            "pass": not negative_unplanned,
            "detail": f"negative_rows={len(negative_unplanned)}",
        },
        "metric_nulls_and_exact_rounding": {
            "pass": not metric_errors,
            "detail": f"ratio_or_null_mismatches={len(metric_errors)}",
        },
        "production_day_floor": {
            "pass": not floor_errors,
            "detail": f"pre_floor_rows={len(floor_errors)}",
        },
        "keys_subset_calendar_with_production": {
            "pass": not subset_errors,
            "detail": f"keys_without_calendar_or_production={len(subset_errors)}",
        },
    }


def render_bcp(rows):
    normalized = []
    for row in rows:
        item = dict(row)
        item["production_day"] = date_value(item["production_day"]).isoformat()
        normalized.append(item)
    normalized.sort(key=lambda row: tuple(
        date_value(row[key]) if key == "production_day" else row[key] for key in KEYS
    ))
    lines = oracle.bcp_lines(normalized)
    return (",".join(COLUMNS) + "\r\n" + "".join(line + "\r\n" for line in lines)).encode()


def sha256(content):
    return hashlib.sha256(content).hexdigest()


def run_reconcile(namespace):
    env = dict(os.environ)
    env["LAKE_ROOT"] = str(LAKE_ROOT)
    env["PYTHONPATH"] = str(ROOT / "lakehouse" / "src")
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "reconcile.py"),
         "--report", REPORT, "--ns", namespace],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=env,
    )
    return result.returncode, result.stdout + result.stderr


def rows_changed(left, right):
    left_map, right_map = row_map(left), row_map(right)
    return sum(
        1 for key in set(left_map) | set(right_map)
        if key not in left_map or key not in right_map
        or any(left_map[key][col] != right_map[key][col] for col in COLUMNS)
    )


def mutate_corrupt_value(spark, source_ns, corrupt_ns):
    from pyspark.sql import functions as F

    source = spark.read.parquet(str(curated_dir(REPORT, source_ns)))
    first = source.select(*KEYS).orderBy(*KEYS).first()
    condition = F.lit(True)
    for key in KEYS:
        condition = condition & (F.col(key) == F.lit(first[key]))
    corrupted = source.withColumn(
        "planned_min",
        F.when(condition, F.col("planned_min") + F.lit(1))
        .otherwise(F.col("planned_min")),
    )
    destination = curated_dir(REPORT, corrupt_ns)
    destination.parent.mkdir(parents=True, exist_ok=True)
    corrupted.write.mode("overwrite").parquet(str(destination))


def checker_self_test(rows, expected_rows, stages):
    """Ensure one deliberately corrupted result trips each output invariant."""
    good = [dict(row) for row in rows]
    planned = stages["planned_time"]
    candidates = [row for row in good if planned[key_of(row)]["planned_dt_min"] > 0]
    if not candidates:
        raise AssertionError("canonical data lacks a planned-downtime row for the mutant")
    planned_row = candidates[0]
    planned_values = planned[key_of(planned_row)]

    metric_row = next(
        row for row in good
        if any(row[metric] is not None for metric in METRICS)
    )
    changed_metric = next(
        metric for metric in METRICS if metric_row[metric] is not None
    )
    rounding_mutant = Decimal("0.0001")
    if metric_row[changed_metric] < Decimal("99999.9999"):
        rounding_value = metric_row[changed_metric] + rounding_mutant
    else:
        rounding_value = metric_row[changed_metric] - rounding_mutant
    null_row = next(row for row in good if row["availability"] is not None)

    mutants = {}
    mutants["drop_row"] = good[1:]
    mutants["duplicate_key"] = good + [dict(good[0])]
    planned_bad = [dict(row) for row in good]
    planned_bad[planned_bad.index(planned_row)]["planned_min"] = (
        planned_values["shift_minutes"] - planned_values["planned_dt_min"]
    )
    mutants["planned_min_subtracts_planned_downtime"] = planned_bad
    rounded_bad = [dict(row) for row in good]
    rounded_bad[rounded_bad.index(metric_row)][changed_metric] = rounding_value
    mutants["off_by_one_rounding"] = rounded_bad
    null_bad = [dict(row) for row in good]
    null_bad[null_bad.index(null_row)]["availability"] = None
    mutants["metric_flipped_to_null"] = null_bad
    floor_bad = [dict(row) for row in good]
    before = dict(good[0])
    before["production_day"] = date(2025, 10, 19)
    floor_bad.append(before)
    mutants["pre_floor_row"] = floor_bad
    extra_bad = [dict(row) for row in good]
    extra = dict(good[0])
    extra["line_id"] = "ZZZ-L9"
    extra_bad.append(extra)
    mutants["extra_key"] = extra_bad

    report = {}
    for name, mutant in mutants.items():
        checks = invariant_checks(mutant, expected_rows, stages)
        tripped = [check for check, outcome in checks.items() if not outcome["pass"]]
        report[name] = {"caught": bool(tripped), "tripped": tripped}
    return report


def canonical_checks(job, spark, tag):
    namespace = f"fuzz-{tag}-canonical"
    job.run(namespace, CANON_AS_OF, spark)
    rows, schema, frame = records_from_parquet(namespace)
    reconcile_code, reconcile_output = run_reconcile(namespace)

    oracle_stages = {}
    oracle_rows = oracle.oee_shift(RAW, parse_ts(CANON_AS_OF), mode="exact", stages=oracle_stages)
    checks = invariant_checks(rows, oracle_rows, oracle_stages)
    checks["schema_contract"] = schema_contract(schema)

    snapshot = SNAPSHOT.read_bytes()
    rendered = render_bcp(rows)
    snapshot_lines = snapshot.decode("utf-8").splitlines()
    rendered_lines = rendered.decode("utf-8").splitlines()
    snapshot_by_key = {tuple(line.split(",")[:4]): line for line in snapshot_lines[1:]}
    rendered_by_key = {tuple(line.split(",")[:4]): line for line in rendered_lines[1:]}
    out = {
        "reconcile": {
            "pass": reconcile_code == 0,
            "exit_code": reconcile_code,
            "output": reconcile_output,
        },
        "byte_compare": {
            "pass": rendered == snapshot,
            "snapshot_sha256": sha256(snapshot),
            "lake_csv_sha256": sha256(rendered),
            "snapshot_bytes": len(snapshot),
            "lake_bytes": len(rendered),
            "snapshot_rows": len(snapshot_lines) - 1,
            "lake_rows": len(rendered_lines) - 1,
            "matched_keys": len(set(snapshot_by_key) & set(rendered_by_key)),
            "missing_keys": len(set(snapshot_by_key) - set(rendered_by_key)),
            "extra_keys": len(set(rendered_by_key) - set(snapshot_by_key)),
            "mismatched_rows": sum(
                snapshot_by_key[key] != rendered_by_key[key]
                for key in set(snapshot_by_key) & set(rendered_by_key)
            ),
            "line_terminator": "CRLF" if b"\r\n" in snapshot else "LF",
        },
        "schema": checks,
    }

    # Re-run the same job and compare sorted exact BCP bytes and pandas dtypes.
    job.run(namespace, CANON_AS_OF, spark)
    rows_again, _, frame_again = records_from_parquet(namespace)
    deterministic = (
        render_bcp(rows_again) == rendered
        and rows_changed(rows, rows_again) == 0
        and list(frame.dtypes) == list(frame_again.dtypes)
    )
    out["rerun_deterministic"] = {"pass": deterministic}

    early_ns = f"fuzz-{tag}-early"
    job.run(early_ns, EARLY_AS_OF, spark)
    early_rows, _, _ = records_from_parquet(early_ns)
    early_stages = {}
    early_expected = oracle.oee_shift(
        RAW, parse_ts(EARLY_AS_OF), mode="exact", stages=early_stages
    )
    early_checks = invariant_checks(early_rows, early_expected, early_stages)
    early_code, _ = run_reconcile(early_ns)
    changed = rows_changed(rows, early_rows)
    early_pass = (
        changed > 0 and early_code != 0
        and all(check["pass"] for check in early_checks.values())
    )
    out["earlier_cutoff"] = {
        "as_of_utc": EARLY_AS_OF,
        "changed_vs_canonical": changed,
        "identical_to_canonical": changed == 0,
        "reconcile_exit_code": early_code,
        "pass": early_pass,
        "note": (
            "Expected reconcile failure follows the legacy flow: the imported "
            "KAN-8 downtime stage applies AsOfUtc before planned-time aggregation."
        ),
    }

    corrupt_ns = f"fuzz-{tag}-corrupt"
    mutate_corrupt_value(spark, namespace, corrupt_ns)
    corrupt_code, corrupt_output = run_reconcile(corrupt_ns)
    out["corrupted_value"] = {
        "pass": corrupt_code != 0,
        "reconcile_exit_code": corrupt_code,
        "mutation": "first key planned_min += 1",
        "output_tail": "\n".join(corrupt_output.strip().splitlines()[-3:]),
    }
    out["_checker_self_test"] = checker_self_test(rows, oracle_rows, oracle_stages)
    return out


def env_info(spark, seed, variant_count, started_utc):
    import pyspark
    import pandas as pd

    jvm = spark.sparkContext._jvm
    git = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return {
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "spark": spark.version,
        "java": jvm.System.getProperty("java.version"),
        "jvm_tzdb": str(
            jvm.java.time.zone.ZoneRulesProvider.getVersions("UTC").lastKey()
        ),
        "pandas": pd.__version__,
        "platform": platform.platform(),
        "spark_session_tz": spark.conf.get("spark.sql.session.timeZone"),
        "as_of_utc": CANON_AS_OF,
        "fuzz_seed": seed,
        "variants": variant_count,
        "seed_py_SEED": 20251020,
        "git_commit": git.stdout.strip(),
        "started_utc": started_utc,
    }


def variant_run(job, spark, raw_dir, namespace, as_of):
    previous_raw = os.environ.get("RAW_DIR")
    os.environ["RAW_DIR"] = str(raw_dir)
    try:
        job.run(namespace, as_of, spark)
    finally:
        if previous_raw is None:
            os.environ.pop("RAW_DIR", None)
        else:
            os.environ["RAW_DIR"] = previous_raw


def fuzz_variant(job, spark, seed, index, raw_workspace):
    rng_seed = seed + index
    rng = random.Random(rng_seed)
    raw_dir = raw_workspace / f"variant-{index:03d}"
    raw_dir.mkdir(parents=True)
    as_of, features, window = make_variant(raw_dir, index, rng)
    namespace = f"fuzz-{seed}-v{index:03d}"
    variant_run(job, spark, raw_dir, namespace, as_of)
    rows, schema, _ = records_from_parquet(namespace)
    stages = {}
    expected = oracle.oee_shift(
        raw_dir, parse_ts(as_of), mode="exact", stages=stages
    )
    checks = invariant_checks(rows, expected, stages)
    checks["schema_contract"] = schema_contract(schema)
    violations = [name for name, check in checks.items() if not check["pass"]]
    _, production = read_csv(raw_dir / "mes.production_count.csv")
    _, events = read_csv(raw_dir / "mes.downtime_event.csv")
    return {
        "variant": index,
        "rng_seed": rng_seed,
        "calendar_window": window,
        "as_of_utc": as_of,
        "pass": not violations,
        "violations": violations,
        "features": features,
        "inputs": {
            "rows": len(production) + len(events),
            "null_rows": 0,
            "open_events": sum(row["end_utc"] == "" for row in events),
            "output_rows": len(rows),
        },
        "invariants": checks,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=25)
    parser.add_argument("--seed", type=int, default=20251117)
    parser.add_argument("--log", default=str(ROOT / "out" / "validation" / "fuzz_oee_shift.json"))
    args = parser.parse_args(argv)
    if args.n < 1:
        parser.error("--n must be positive")
    return args


def main(argv=None):
    started_utc = datetime.now(timezone.utc).strftime(TS)
    args = parse_args(argv)
    tag = str(args.seed)
    raw_workspace = ROOT / "out" / f"fuzz-{tag}-raw"
    log_path = Path(args.log)
    namespaces = [
        f"fuzz-{tag}-canonical",
        f"fuzz-{tag}-early",
        f"fuzz-{tag}-corrupt",
        *(f"fuzz-{tag}-v{index:03d}" for index in range(args.n)),
    ]
    collisions = [str(LAKE_ROOT / namespace) for namespace in namespaces
                  if (LAKE_ROOT / namespace).exists()]
    if raw_workspace.exists() or collisions:
        raise FileExistsError(
            f"refusing to overwrite existing fuzz artifacts: "
            f"{[str(raw_workspace)] if raw_workspace.exists() else []} {collisions}"
        )
    if log_path.exists():
        raise FileExistsError(f"refusing to overwrite existing fuzz log: {log_path}")
    raw_workspace.mkdir(parents=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    original_raw = os.environ.get("RAW_DIR")
    os.environ.pop("RAW_DIR", None)
    spark = get_spark(f"fuzz.{REPORT}.{tag}")
    job = __import__("mfg_lake.jobs.oee_shift", fromlist=["run"])
    results = []
    try:
        canonical = canonical_checks(job, spark, tag)
        for index in range(args.n):
            result = fuzz_variant(
                job, spark, args.seed, index, raw_workspace
            )
            results.append(result)
            print(
                f"variant {index:03d}: "
                f"{'PASS' if result['pass'] else 'FAIL'} "
                f"as_of={result['as_of_utc']} rows={result['inputs']['output_rows']} "
                f"violations={result['violations']}"
            )

        checker_results = canonical.pop("_checker_self_test")
        checker_pass = all(item["caught"] for item in checker_results.values())
        canonical_pass = (
            canonical["reconcile"]["pass"]
            and canonical["byte_compare"]["pass"]
            and canonical["rerun_deterministic"]["pass"]
            and canonical["corrupted_value"]["pass"]
            and canonical["earlier_cutoff"]["pass"]
            and all(item["pass"] for item in canonical["schema"].values())
        )
        overall = canonical_pass and checker_pass and all(item["pass"] for item in results)
        report = {
            "report_meta": REPORT_META,
            "environment": env_info(spark, args.seed, args.n, started_utc),
            "canonical": canonical,
            "checker_self_test": checker_results,
            "variants": results,
            "summary": {
                "overall_pass": overall,
                "variants_total": len(results),
                "variants_passed": sum(item["pass"] for item in results),
                "variants_failed": sum(not item["pass"] for item in results),
                "checker_mutants_total": len(checker_results),
                "checker_mutants_caught": (
                    f"{sum(item['caught'] for item in checker_results.values())}"
                    f"/{len(checker_results)}"
                ),
            },
        }
        log_path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
        print(
            f"overall_pass={overall} variants={len(results)} "
            f"mutants={len(checker_results)} log={log_path}"
        )
        return 0 if overall else 1
    finally:
        spark.stop()
        if original_raw is None:
            os.environ.pop("RAW_DIR", None)
        else:
            os.environ["RAW_DIR"] = original_raw
        shutil.rmtree(raw_workspace, ignore_errors=True)
        for namespace in namespaces:
            shutil.rmtree(LAKE_ROOT / namespace, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
