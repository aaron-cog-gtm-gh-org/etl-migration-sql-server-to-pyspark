# PL_Scrap_Yield -> `mfg_lake.jobs.scrap_yield_weekly` (KAN-9)

Legacy: `legacy/adf/pipeline/PL_Scrap_Yield.json` (alloc proc + rpt proc +
the Copy to curated). Target: `lakehouse/src/mfg_lake/jobs/scrap_yield_weekly.py`,
pipeline `lakehouse/adf/pipeline/PL_Scrap_Yield.json`, curated report
`abfss://curated@kcmfglake.dfs.core.windows.net/manufacturing/scrap_yield_weekly`.

Grain: `(plant_id, line_id, iso_year, iso_week)` — one row per line per ISO
week that has any staged production. Output columns:
`plant_id` CHAR(5) -> trimmed `string`, `line_id` VARCHAR(12) -> `string`,
`iso_year`/`iso_week`/`good_units`/`scrap_units` INT -> `int32`,
`scrap_pct` DECIMAL(9,2) NULL -> `decimal(9,2)`.

## Step mapping

| # | legacy step | PySpark equivalent | test(s) |
|---|---|---|---|
| 1 | `mes.usp_allocate_scrap_to_orders`: cursor over `mes.scrap_event` ORDER BY scrap_id; `#ov` = orders on the same line with `sched_start < ts+7200s`, `sched_end > ts-7200s`, `ts BETWEEN sched_start AND sched_end`; `ov_secs = DATEDIFF(SECOND, GREATEST(start, ts-7200), LEAST(end, ts+7200))` | `allocate_scrap_to_orders()` (set-based): same predicates as a join, `ov_secs` from `least`/`greatest` on unix seconds | `test_alloc_*` |
| 2 | floor per order: `CAST(qty * ov_secs * 1.0 / tot AS INT)` | `qty * ov_secs div tot` on BIGINT (positive operands: truncating == floor) | `test_alloc_proportional_*`, `test_alloc_window_clipped_to_7200s` |
| 3 | remainder UPDATE: `TOP 1 ... ORDER BY ov_secs DESC, order_id` gets `qty - SUM(alloc)` | `row_number() OVER (PARTITION BY scrap_id ORDER BY ov_secs DESC, order_id) = 1` gets the remainder | `test_alloc_tie_broken_by_order_id_string`, `test_alloc_remainder_not_spread` |
| 4 | no candidate -> `('UNALLOCATED', qty)` | left-anti join + literal `UNALLOCATED` row | `test_alloc_other_line_ignored_unallocated` |
| 5 | `stg.scrap_alloc` is written by the proc | **not persisted**: the rpt proc reads `mes.scrap_event` directly, so alloc is computed and logged in `run()` only (parity of computation without the dead table) | `test_alloc_matches_cursor_port_on_seed` |
| 6 | `dim.usp_refresh_shift_calendar` + `mes.usp_stg_production_counts` -> `stg.production_local` | imported `build_shift_calendar()` / `stage_production_local()` from `mfg_lake.jobs.daily_production` (KAN-6 contract: do not re-derive) | `tests/test_daily_production.py` |
| 7 | `good`: `stg.production_local` grouped by `DATEPART(ISO_WEEK, production_day)` and `YEAR(DATEADD(DAY, 26 - ISO_WEEK, production_day))`; **no `dim.sku` join** | `stage_good_weekly()`: `iso_year_week(production_day)`, `SUM(good_units)`; unknown SKUs count, as legacy | `test_unknown_sku_counts_toward_good`, `test_production_day_monday_0600` |
| 8 | `scrap`: `mes.scrap_event JOIN dim.line`, grouped by `DATEPART(ISO_WEEK, scrap_ts_utc)` — the **UTC** timestamp, not the local day | `stage_scrap_weekly()`: `iso_year_week(scrap_ts_utc)`; inner join to `dim.line` | `test_scrap_week_uses_utc`, `test_unknown_line_scrap_dropped` |
| 9 | `iso_year = YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, d), d))` (legacy trick giving the ISO-8601 year) | `iso_year_week(col)`: `weekofyear(to_date(col))` for the week; `year(date_add(to_date(col), 26 - wk))` for the year — same trick | `test_iso_year_week_around_jan_1` |
| 10 | `good LEFT JOIN scrap` on the 4 keys — scrap-only weeks are **dropped** | `build_report()`: same left join; `scrap_units = ISNULL(s,0)` -> `coalesce(...,0)` | `test_left_join_drops_scrap_only_week`, `test_week_without_scrap_reports_zero` |
| 11 | `ROUND(CAST(scrap AS DECIMAL(18,4)) / NULLIF(good + scrap,0) * 100, 2)` | decimal division, NULL when `good+scrap = 0`, `round(..,2)` (HALF_UP = T-SQL half away from zero), cast `decimal(9,2)` | `test_scrap_pct_rounding` (0.125 -> 0.13; 12.345 -> 12.35), `test_scrap_pct_null_when_den_zero` |
| 12 | `TRUNCATE` + `INSERT rpt.scrap_yield_weekly`, then `COPY_rpt_scrap_yield_weekly_to_curated` (Parquet sink) | `common.io.write_curated(df, "scrap_yield_weekly", ns)` (overwrite, parquet) | `make reconcile REPORT=scrap_yield_weekly` |
| 13 | `etl.usp_log_run` start/finish rows | stdout run line (inputs, dropped rows, staged rows, alloc rows, dropped groups, report rows) | - |
| 14 | `AsOfUtc` pipeline parameter (declared, never passed to the procs) | `--as-of-utc` accepted, format-validated, not applied | `test_cli_*`; validation (earlier cutoff -> identical output) |

## Hazards (legacy behaviours that must survive)

- **UTC vs local week for scrap.** Scrap is bucketed by the ISO week of the
  UTC `scrap_ts_utc`. A port that buckets by local time (e.g. Chicago
  Sunday 22:00 CST = Monday 03:00 UTC -> week 44, not 43) mismatches 33/96
  rows of the snapshot. Good units use the local `production_day` of
  `stg.production_local` — the two sides use different clocks.
- **`ts BETWEEN sched_start AND sched_end` is inclusive** — an order that
  starts or ends exactly at the scrap timestamp is a candidate.
- **The ±7200 window is clipped**: `ov_secs` never exceeds the intersection
  of `[sched_start, sched_end]` with `[ts-2h, ts+2h]`, so a long order
  saturates at 14400 s.
- **Tie-break is a string compare**: `ORDER BY ov_secs DESC, order_id` on a
  VARCHAR column puts `'ORD-10'` before `'ORD-2'`.
- **`stg.production_local` has no `dim.sku` join** — production buckets for
  unknown SKUs still count toward `good_units` (unlike KAN-6's report,
  which joins `dim.sku`).
- **Left join drops scrap-only weeks**: a scrap event in a week with no
  production for that line produces no report row (dropped groups/units are
  logged).
- **`scrap_pct` arithmetic is decimal**: `DECIMAL(18,4) / DECIMAL(10,0) *
  100` then `ROUND(,2)` — a float port rounds 12.345 to 12.34, legacy gives
  12.35; the exact-tie 0.125 gives 0.13 (not banker's 0.12).
- **NULL `scrap_pct`** when `good_units + scrap_units = 0`; 100.00 when
  `good_units = 0` and scrap > 0.

## Deliberate deviations / decisions (need sign-off)

1. **NULL inputs are dropped and counted** (`dropped_not_null`), like
   KAN-6: the source columns are NOT NULL so legacy never sees them.
2. **INT overflow raises `ArithmeticError`** (`qty * ov_secs`, `SUM(ov)`,
   `good+scrap`, and the report's unit columns) — SQL Server raises
   *Arithmetic overflow*; Spark would silently wrap.
3. **`stg.scrap_alloc` is not persisted.** The rpt proc reads
   `mes.scrap_event` directly; the allocation is recomputed in `run()` (and
   checked against the cursor port in tests/fuzz) for parity and logging
   only. Legacy's Copy exports the rpt table only.
4. **`--as-of-utc` is validated but not applied (KAN-9 AC4 conflict).**
   Per the requester's instruction and the legacy pipeline (the procs
   receive no AsOfUtc), an earlier cutoff produces *identical* output and
   the same reconcile result. AC4's "earlier cutoff makes reconcile fail"
   is therefore **not met by design** — user-approved, same as KAN-6 AC4.
5. **`stg.production_local` is re-derived in-job** via the KAN-6 public
   functions, not read back — same as legacy, where the rpt proc trusts the
   staging table the alloc step just left behind.
6. **Duplicate feed rows are summed**, as legacy has no PK on
   `mes.production_count` (production_order's PK does dedupe order ids in
   theory; the job keeps the join semantics either way).

## Ticket conflicts / AC notes

- **AC3** (canonical reconcile green): PR #5's seed added two `SKU-XX99`
  production_count rows (good_units 2350 each) which count toward
  `good_units` here because the rpt proc has no `dim.sku` join — without
  the seed fix, reconcile fails on exactly 2 rows (`(PLT02,PLT02-L3,2025,43)`
  and `(PLT05,PLT05-L1,2025,45)`, each +2350); everything else matches
  (oracle-verified). The user-approved fix — zeroing the units on those two
  rows (kept as unknown-SKU rows, so `daily_production` still drops them
  and its snapshot is unchanged) — lands on PR #5's branch
  (`devin/1791337176-daily-production`), not this one. With it applied,
  reconcile is 96/96 with 0 diffs.
- **AC4** (earlier cutoff makes reconcile fail): not met by design,
  user-approved; see deviation 4.

## Open questions

- `SKU-XX99` seed rows: **resolved** — kept as rows but zeroed in PR #5's
  seed fix, so `stg.production_local` consumers keep snapshot parity
  (see AC3).
- The `±7200` in the alloc proc is read as **seconds** (the DATEADD bounds
  in the same WHERE clause). No alternative reading was found that matches
  the cursor-port oracle on the seed; confirm if a dissenting legacy
  interpretation exists.
- `order_id` tie-break collation: legacy `ORDER BY order_id` uses the
  server collation (typically CI). The job compares the raw string; seed
  order ids are uppercase+numeric so this cannot differ on current data.

## Validation

_To be filled by the fuzz/validation run (tools/fuzz_scrap_yield_weekly.py,
out/validation/scrap_yield_weekly_validation.html)._

## Sign-off

| role | name | date |
|---|---|---|
| Data engineering | _pending_ | |
| Reporting owner | _pending_ | |
