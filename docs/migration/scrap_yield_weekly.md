# Source-to-target mapping: `rpt.scrap_yield_weekly` → curated `scrap_yield_weekly`

| | Legacy | Target |
|---|---|---|
| Orchestration | ADF `PL_Scrap_Yield` (`legacy/adf/pipeline/`): `SP_AllocateScrapToOrders` → `SP_RptScrapYieldWeekly` → `COPY_rpt_scrap_yield_weekly_to_curated`; `PL_Master` runs it after `EP_PL_Daily_Production` | ADF `PL_Scrap_Yield` (`lakehouse/adf/pipeline/`): single `SPK_ScrapYieldWeekly` (DatabricksSparkPython) |
| Code | `mes.usp_allocate_scrap_to_orders`, `rpt.usp_rpt_scrap_yield_weekly`; reads `stg.production_local` from `mes.usp_stg_production_counts` (`PL_Daily_Production`) | `lakehouse/src/mfg_lake/jobs/scrap_yield_weekly.py`; imports `mfg_lake.jobs.daily_production.stg_production_local` |
| Parameter | `AsOfUtc` (pipeline parameter; **not passed to either proc**) | `--as-of-utc`: validated with `parse_as_of_utc` and logged, otherwise unused (H8) |
| Output | `rpt.scrap_yield_weekly` → `abfss://curated@nwhmfglake.dfs.core.windows.net/manufacturing/scrap_yield_weekly` | same path; parquet, **full overwrite** (≡ `TRUNCATE`+`INSERT`+Copy) |
| Side stage | `stg.scrap_alloc` (DROP/CREATE every run) | `stg_scrap_alloc()` DataFrame. Computed and row counts logged, **not written**: the legacy Copy exports only `rpt.scrap_yield_weekly`, and no proc in `legacy/` reads `stg.scrap_alloc` |

## Grain

One row per `(plant_id, line_id, iso_year, iso_week)` from the `good` CTE (the left side of the `LEFT JOIN`). Reconcile keys in `tools/reconcile_config.yaml` match.

## Inputs (DDL from `legacy/sql/schema/`)

| table | columns used | notes |
|---|---|---|
| `mes.scrap_event` | `scrap_id INT` PK, `line_id VARCHAR(12)`, `scrap_ts_utc DATETIME2(0)`, `qty_units INT` | both procs |
| `mes.production_order` | `order_id VARCHAR(14)` PK, `line_id VARCHAR(12)`, `sched_start_utc`/`sched_end_utc DATETIME2(0)` | allocation proc only |
| `dim.line` | `line_id VARCHAR(12)` PK, `plant_id CHAR(5)` | inner join to scrap (report) |
| `stg.production_local` | `line_id`, `plant_id CHAR(5)`, `production_day DATE`, `good_units INT` | from `stg_production_local()` (+ `mes.production_count`, `dim.line`, `dim.plant`, `dim.shift_pattern`, `dim.calendar`) |

## Output columns (`rpt.sql` / MANIFEST)

| target column | legacy type → parquet | derivation |
|---|---|---|
| `plant_id` | `CHAR(5)` → `string` | `stg.production_local.plant_id`, trimmed |
| `line_id` | `VARCHAR(12)` → `string` | `stg.production_local.line_id` |
| `iso_year` | `INT` → `int` | `YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, d), d))`, `d = CAST(production_day AS DATETIME)` |
| `iso_week` | `INT` → `int` | `DATEPART(ISO_WEEK, production_day)` |
| `good_units` | `INT` → `int` | `SUM(stg.production_local.good_units)` |
| `scrap_units` | `INT` → `int` | `ISNULL(SUM(mes.scrap_event.qty_units), 0)`, grouped by the **UTC** ISO week of `scrap_ts_utc` |
| `scrap_pct` | `DECIMAL(9,2) NULL` → `decimal(9,2)` | `ROUND(CAST(scrap AS DECIMAL(18,4)) / NULLIF(good + scrap, 0) * 100, 2)` |

## Stages

1. `mes.usp_allocate_scrap_to_orders` → `stg_scrap_alloc(scrap_event, production_order)`: cursor over `scrap_id` re-expressed set-based (each event is independent). Output `(scrap_id int, order_id string, qty_units int)`.
2. `mes.usp_stg_production_counts` → imported `daily_production.stg_production_local(...)` (not re-derived).
3. `rpt.usp_rpt_scrap_yield_weekly` → `rpt_scrap_yield_weekly(scrap_event, line, stg_production_local)`. `etl.usp_log_run` (open row / close with `@@ROWCOUNT` / `FAILED` + `THROW`) → job logging + non-zero exit.

## SQL Server semantic hazards

### Report (`rpt.usp_rpt_scrap_yield_weekly`)

| # | hazard | PySpark equivalent |
|---|---|---|
| H1 | Scrap is bucketed by ISO week of the **UTC** `scrap_ts_utc`; good units by ISO week of the **local** `production_day`. A Sunday-evening local scrap lands in the next week | `weekofyear(to_date(scrap_ts_utc))` with the session in UTC. Seed: 18 events have a UTC week different from their local production-day week. 32 of 96 snapshot rows would differ on a local-week port, so the snapshot confirms the UTC rule |
| H2 | ISO year via `YEAR(DATEADD(DAY, 26 - ISO_WEEK, d))` | `year(date_add(d, 26 - weekofyear(d)))`; Spark `weekofyear` is ISO-8601. Tested around Jan 1 (2024-12-30 → 2025-W01, 2021-01-03 → 2020-W53, 2027-01-01 → 2026-W53) |
| H3 | `good LEFT JOIN scrap`: scrap weeks with no production row are dropped silently; weeks without scrap get `ISNULL(..., 0)` | same join direction; dropped scrap groups (count + sample keys) logged; `coalesce(scrap, 0)` |
| H4 | `mes.scrap_event JOIN dim.line` (inner): scrap on unknown lines dropped | same join; dropped count + sample `scrap_id`s logged |
| H5 | `CAST(.. AS DECIMAL(18,4)) / NULLIF(INT, 0) * 100`, `ROUND(.., 2)` half away from zero, into `DECIMAL(9,2)`; `NULLIF` → NULL | decimal maths as in `daily_production` (`decimal(18,4)` / `decimal(10,0)` × `decimal(3,0)`), `round` on decimal (HALF_UP), cast `decimal(9,2)`; NULL when `good + scrap = 0`. Never on double |
| H6 | `SUM(int)` is INT (overflow → error); `good_units + ISNULL(scrap, 0)` is INT addition (overflow → error) | sums and the denominator checked against the INT range; raise `OverflowError` |
| H7 | `stg.production_local` semantics (TZ, production day, shift join) | imported stage; covered by `tests/test_daily_production.py` |
| H8 | **No `@AsOfUtc`** in either proc | parity: no cutoff filter. `--as-of-utc` validated and logged only; an earlier cutoff still reconciles (as in `daily_production` H10) |
| H9 | `CHAR(5)` `plant_id` padding; VARCHAR keys | trimmed before joins and in output |

### Allocation (`mes.usp_allocate_scrap_to_orders` → `stg.scrap_alloc`)

| # | hazard | PySpark equivalent |
|---|---|---|
| A1 | `CURSOR ... ORDER BY scrap_id` with per-row `#ov` temp table | set-based: join scrap × orders on line, window per `scrap_id` |
| A2 | candidate filter `@ts BETWEEN sched_start AND sched_end` (inclusive both ends), plus `start < ts + 7200 s AND end > ts - 7200 s` (implied by the BETWEEN) | same predicates, inclusive |
| A3 | `CASE WHEN o.sched_start_utc > @ts - 7200 ...` with `@ts DATETIME2(0)`: Msg 206 in SQL Server (`DATETIME2 ± int` is an operand type clash) | **approved deviation**: the intended ±7200 s window, `ov_secs = unix_timestamp(least(end, ts + 7200 s)) - unix_timestamp(greatest(start, ts - 7200 s))` (see Deviations) |
| A4 | `DATEDIFF(SECOND, a, b)` counts second boundaries | whole-second inputs (0 sub-second values in seed) → `unix_timestamp(b) - unix_timestamp(a)` |
| A5 | `CAST(@qty * ov_secs * 1.0 / @tot AS INT)`: `@qty * ov_secs` is INT × INT (overflow → error), then decimal division, CAST truncates toward zero | overflow check, then long `div` (truncates toward zero; equal to the decimal path for integer inputs) |
| A6 | remainder `@qty - SUM(floor)` goes to `TOP 1 ... ORDER BY ov_secs DESC, order_id` | `row_number() over (partition by scrap_id order by ov_secs desc, order_id asc)`; `order_id` is always `ORD-dddddd` in seed, so the server collation and binary order agree |
| A7 | `@tot = 0` (all candidates have zero overlap) → divide-by-zero error, proc fails | raise `ZeroDivisionError` |
| A8 | no candidate order → one row `('UNALLOCATED', @qty)` | same |
| A9 | `stg.scrap_alloc` has no consumer in `legacy/` and no snapshot | stage parity rests on unit tests only |

## Seed-data coverage (`make seed`)

| measure | count |
|---|---|
| `mes.scrap_event` rows / `qty_units` range | 684 / 5–399 (none negative) |
| scrap events with 0 / 1 / 2 candidate orders | 446 / 184 / 54 |
| scrap events exactly on an order start/end (BETWEEN boundary) | 0 |
| scrap events whose UTC ISO week ≠ local production-day ISO week | 18 |
| scrap groups dropped by the `LEFT JOIN` / scrap on unknown lines | 0 / 0 |
| report rows with `scrap_units = 0` / `scrap_pct` NULL / exact-half `scrap_pct` | 0 / 0 / 0 |
| ISO weeks in the snapshot | 2025-W43 … W46 (no year boundary) |
| sub-second timestamps, padded ids | 0 / 0 |

Not exercised by the seed (unit tests only): Jan-1 ISO year, scrap weeks without production, unknown lines, zero denominator, exact-half rounding, INT overflow (both procs), BETWEEN boundaries, `@tot = 0`, three-way overlap ties. The seed does exercise the allocation remainder and the two-way `order_id` tie-break (11 tied multi-order events) and the `UNALLOCATED` fallback (446 events).

## Implementation

`lakehouse/src/mfg_lake/jobs/scrap_yield_weekly.py` has the following functions:
- `stg_scrap_alloc(scrap_event, production_order)` is the set-based allocation (A1–A8). It raises `OverflowError` or `ZeroDivisionError` where the proc would fail.
- `rpt_scrap_yield_weekly(scrap_event, line, stg)` builds the report (H1–H6, H9).
- `iso_week_and_year(d)` computes the ISO week and ISO year (H2).
- `build_report(frames, as_of_utc)` wires the stages:
  - it builds `plants_with_iana` → `build_shift_calendar` → the imported `daily_production.stg_production_local` (H7);
  - it computes and logs `stg.scrap_alloc`, with a qty-conservation check;
  - it validates and logs the unused cutoff (H8).
- `run()` full-overwrites the curated parquet.

`mfg_lake/common/`, `tools/reconcile.py` and `tools/reconcile_config.yaml` are unchanged. The reconcile harness already normalises int, string and decimal, so this report needs no harness change.

## Hazard → covering test

All tests are in `tests/test_scrap_yield_weekly.py`.

| Hazard | Covering tests |
|---|---|
| H1 | `test_scrap_week_uses_utc_timestamp_not_local_production_day`, `test_full_seed_report_matches_snapshot_shape` |
| H2 | `test_iso_week_and_year_around_jan_1` (parametrised) |
| H3 | `test_scrap_week_without_production_is_dropped_and_logged`, `test_week_without_scrap_reports_zero` |
| H4 | `test_scrap_on_unknown_line_is_dropped_and_logged` |
| H5 | `test_scrap_pct_decimal_rounding_half_away_from_zero` (parametrised), `test_zero_denominator_scrap_pct_is_null` |
| H6 | `test_int_overflow_raises` |
| H7 | `test_build_report_end_to_end_schema_and_trim`, `test_full_seed_report_matches_snapshot_shape`; stage semantics in `tests/test_daily_production.py` |
| H8 | `test_as_of_is_validated_but_does_not_filter` |
| H9 | `test_build_report_end_to_end_schema_and_trim` |
| A1 | `test_alloc_matches_literal_cursor_port_on_seed` (`exceptAll` both ways vs the literal cursor port `_alloc_cursor_port`) |
| A2 | `test_alloc_between_is_inclusive`, `test_alloc_other_line_orders_ignored_and_unallocated_logged` |
| A3 | `test_alloc_window_clipped_to_7200_seconds` (±7200 s gives 14/5; the full-duration reading would give 18/1) |
| A4 | `test_alloc_window_clipped_to_7200_seconds`, `test_alloc_between_is_inclusive` |
| A5 | `test_alloc_remainder_to_largest_overlap` (truncation), `test_alloc_int_overflow_raises` |
| A6 | `test_alloc_remainder_to_largest_overlap`, `test_alloc_tie_broken_by_order_id`, `test_alloc_remainder_not_spread` |
| A7 | `test_alloc_zero_total_overlap_raises` |
| A8 | `test_alloc_between_is_inclusive`, `test_alloc_other_line_orders_ignored_and_unallocated_logged` |
| A9 | `test_alloc_single_order_full_qty`, `test_alloc_trims_ids_and_schema`, `test_alloc_matches_literal_cursor_port_on_seed` |
| wall clock | `test_job_source_never_reads_wall_clock` |

## Dropped / filtered row logging

| Join / stage | Seed-run result (`docs/migration/evidence/scrap_yield_weekly/dev_run.log`) |
|---|---|
| imported stage `dim.line` / `dim.plant` / `dim.shift_calendar` inner joins | 0 / 0 / 0 buckets dropped; 61,336 → 61,336 staged |
| `mes.scrap_event JOIN dim.line` (H4) | 0 scrap events dropped; 684 → 684 |
| scrap groups with no good row at `good LEFT JOIN scrap` (H3) | 0 dropped |
| `stg.scrap_alloc` | 738 rows for 684 events (292 allocated to orders, 446 `UNALLOCATED`, sample scrap_ids `[400001, 400002, 400003, 400007, 400008]`); quantity conserved |
| report | 96 rows written |

## Adversarial checks

| Check | Result |
|---|---|
| `--as-of-utc "2025-11-03 00:00:00"` into `adv_asof` | **PASS** 14/14. Expected, because neither legacy proc reads `AsOfUtc` (H8). Applying a cutoff would break parity |
| Canonical runs into `adv_r1` and `adv_r2` | identical: sorted DataFrames and dtypes equal; both sorted CSVs SHA-256 `951e20a372948fa98cafc156a7591af75b5ab374c346fc9fdcecdba83437cd9a` (3,992 bytes) |
| Increment one `scrap_units` in `adv_corrupt` (`PLT01|PLT01-L1|2025|43`, 1609 → 1610, int32 preserved) | **FAIL** as expected, 12/14, exit 1: `col[scrap_units].checksum` and `col[scrap_units].mismatches n=1` |
| `legacy_snapshots/` SHA-256 before/after | unchanged |

The full output is in `docs/migration/evidence/scrap_yield_weekly/adversarial.log`.

## Reconciliation

Canonical dev reconciliation: 14/14 controls PASS. `make ci` also passes, with line_downtime_daily 13/13, daily_production 13/13 and scrap_yield_weekly 14/14.

```text
PASS  row_count                    lake=96 legacy=96
PASS  key_set                      missing=0 extra=0
PASS  col[plant_id].mismatches     n=0
PASS  col[line_id].mismatches      n=0
PASS  col[iso_year].checksum       lake=194400.0000 legacy=194400.0000
PASS  col[iso_year].mismatches     n=0
PASS  col[iso_week].checksum       lake=4272.0000 legacy=4272.0000
PASS  col[iso_week].mismatches     n=0
PASS  col[good_units].checksum     lake=168959976.0000 legacy=168959976.0000
PASS  col[good_units].mismatches   n=0
PASS  col[scrap_units].checksum    lake=138356.0000 legacy=138356.0000
PASS  col[scrap_units].mismatches  n=0
PASS  col[scrap_pct].checksum      lake=8.2000 legacy=8.2000
PASS  col[scrap_pct].mismatches    n=0
```

## Divergences found and fixed

None: the first reconciliation passed. The UTC-week rule (H1) was confirmed against the snapshot before implementation. A local-week port would mismatch 32 of 96 rows.

## Decisions / reviewer notes

- **A3 (decided by the user, Aaron: option 1).** SQL Server only supports `+`/`-` with an integer on `DATETIME`/`SMALLDATETIME`, where the integer counts days. On `DATETIME2(0)` it raises Msg 206 (operand type clash), so the literal proc would fail on its first scrap event. The approved reading is the intended ±7200 s window: the header says "proportional to overlap minutes", and the `WHERE` already uses `DATEADD(SECOND, ±7200, @ts)`. The alternative "DATETIME days" reading would weight by the full scheduled duration and differs on 108 of 292 allocated seed rows. Everything else in the proc is ported exactly as written: the three `WHERE` predicates including the inclusive `@ts BETWEEN`, the per-order `CAST(@qty * ov_secs * 1.0 / @tot AS INT)` truncation, the whole remainder to `TOP 1 ... ORDER BY ov_secs DESC, order_id`, and the `'UNALLOCATED'` fallback.
- `stg.scrap_alloc` has no consumer and no snapshot. Its parity rests on hand-computed unit tests plus a literal cursor port (test helper) compared with `exceptAll` both ways on the full seed. A3 does not affect `rpt.scrap_yield_weekly` or its reconciliation.
- H8: an earlier `--as-of-utc` still PASSes reconcile, because neither proc reads `AsOfUtc`. That is expected, not a missed filter.
- A6: the `order_id` tie-break assumes the prod collation orders `ORD-dddddd` ids the same as binary order. That holds for every seed id.

## Deviations from legacy

- **A3, approved by the user:** `@ts - 7200` / `@ts + 7200` on `DATETIME2(0)` (Msg 206 in SQL Server) is implemented as ±7200 **seconds**. Scope: `stg.scrap_alloc` only, with no effect on the curated report.
