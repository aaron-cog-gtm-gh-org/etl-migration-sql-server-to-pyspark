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
5. **`stg.production_local` is recomputed in-job, not shared.** The legacy
   rpt proc reads the `stg.production_local` table that the upstream
   `PL_Daily_Production` staged; the job recomputes the same rows through
   KAN-6's public `build_shift_calendar` / `stage_production_local`
   (same rows, no shared table).
6. **Duplicate feed rows are summed**, as legacy has no PK on
   `mes.production_count` (production_order's PK does dedupe order ids in
   theory; the job keeps the join semantics either way).
7. **`±7200` in the alloc proc is read as seconds.** As written,
   `@ts - 7200` / `@ts + 7200` on a `DATETIME2(0)` is an operand type
   clash in SQL Server (the proc would error). It is read as ±7200
   **seconds**, matching the `DATEADD(SECOND, ±7200, @ts)` bounds in the
   same WHERE clause. Approved deviation. It affects only the allocation,
   which does not feed the report.

## Coverage gaps

- `stg.scrap_alloc` has no prod extract, so the allocation is checked
  against the literal cursor port only, not prod.
- `order_id` tie-break collation: legacy `ORDER BY order_id` uses the
  server collation (typically CI); the job compares the raw string. They
  agree for the upper-case/numeric ids in the data, so this cannot differ
  on current data.

## AC status

| AC | status | evidence |
|---|---|---|
| AC1 | met | abfss path exact (`abfss://curated@kcmfglake.dfs.core.windows.net/manufacturing/scrap_yield_weekly`); local path note: the shared `common/paths.curated_dir` (PR #5) maps it to `out/<ns>/curated/manufacturing/scrap_yield_weekly`, which the job uses |
| AC2 | met | tests committed in `2f1faa4` before the job in `e11b58e` |
| AC3 | met | reconcile 96 rows, 0 missing/0 extra keys, 0 mismatches; depends on PR #5's zero-unit `SKU-XX99` seed fix (`419b220`) — without it the only diff is the 2 known `good_units` rows |
| AC4 | partially met | corrupted value fails reconcile; rerun is identical; the earlier-cutoff FAIL is not met by design, user-approved (the procs never receive `AsOfUtc`, so identical output is the parity result) |
| AC5 | met | `PL_Master` untouched; pipeline name, `AsOfUtc` parameter and retry policy kept |
| AC6 | met except sign-off | doc/tests/tools complete; sign-off pending human reviewers |
| AC7 | met | `legacy/` and `legacy_snapshots/` untouched (`git status` clean) |

## Open questions

- `SKU-XX99` seed rows: **resolved** — kept as rows but zeroed in PR #5's
  seed fix, so `stg.production_local` consumers keep snapshot parity
  (see AC3).

## Validation

Recorded run: `tools/run_validation.sh kan9-validation 25 scrap_yield_weekly`
at `d8dcf36` (stacked on PR #5 incl. `419b220`), rendered with
`tools/validation_report.py --report scrap_yield_weekly --embed-video`
(156/156 checks pass).

- **Unit tests:** 95 passed (35 KAN-9) — `out/validation/pytest_scrap_yield_weekly.xml`.
- **Reconcile:** `PASS: scrap_yield_weekly (14/14 controls)` — row_count
  96/96, key_set missing=0 extra=0, every column 0 mismatches, checksums equal,
  `numeric_tolerance` unchanged.
- **Byte-compare:** bcp-format CSV (CRLF) is byte-identical to
  `legacy_snapshots/rpt.scrap_yield_weekly.csv`.
- **Rerun:** deterministic (identical output).
- **Earlier cutoff** (`2025-11-03 00:00:00`): identical output, reconcile
  exit 0 — the expected parity result (AC4 not met by design).
- **Corrupted value:** reconcile exit 1 (expected FAIL).
- **Fuzz:** 25/25 variants pass vs the oracle (report + allocation vs the
  literal cursor port); checker mutants 8/8 caught; `overall_pass: true` —
  `out/validation/fuzz_scrap_yield_weekly.json`. Also passes with
  `--seed 7` and `--seed 20251117`.
- **Snapshots:** `legacy_snapshots/` sha256 unchanged before/after
  (`rpt.scrap_yield_weekly.csv` = `5fc7c1d2…acaf3f2`); `legacy/` untouched.
- **Recording:** `out/validation/fuzz_run_scrap_yield_weekly.{cast,mp4}`,
  embedded in `out/validation/scrap_yield_weekly_validation.html`.

## Sign-off

| role | name | date |
|---|---|---|
| Data engineering | _pending_ | |
| Reporting owner | _pending_ | |
