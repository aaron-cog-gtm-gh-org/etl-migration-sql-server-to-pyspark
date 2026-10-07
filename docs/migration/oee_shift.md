# Source-to-target mapping: `rpt.oee_shift` → curated `oee_shift`

| | Legacy | Target |
|---|---|---|
| Orchestration | ADF `PL_OEE` (`legacy/adf/pipeline/`): `SP_CalcPlannedTime` → `SP_RptOeeShift` → `COPY_rpt_oee_shift_to_curated`; runs after `EP_PL_Line_Downtime` + `EP_PL_Daily_Production` in `PL_Master` | ADF `PL_OEE` (`lakehouse/adf/pipeline/`): single `SPK_OeeShift` (DatabricksSparkPython) |
| Code | `mes.usp_calc_planned_time`, `rpt.usp_rpt_oee_shift`; reads `stg.downtime_shift_seg` (`PL_Line_Downtime`), `stg.production_local` (`PL_Daily_Production`), `dim.shift_calendar` (`dim.usp_refresh_shift_calendar`, `PL_Master`) | `lakehouse/src/mfg_lake/jobs/oee_shift.py` |
| Upstream stages | written to shared `stg.*` tables by the other pipelines | imported, not re-derived: `line_downtime_daily.stage_downtime_local` + `split_by_shift` (≡ `stg.downtime_shift_seg`), `daily_production.stg_production_local` (≡ `stg.production_local`), `common.shift_calendar.plants_with_iana` + `build_shift_calendar` (≡ `dim.shift_calendar`) |
| Parameter | `AsOfUtc` pipeline parameter; **neither OEE proc reads it**, but `stg.downtime_shift_seg` was built by `PL_Line_Downtime` with the same `AsOfUtc` (`start_utc < @AsOfUtc`, open events capped) | `--as-of-utc` → `parse_as_of_utc` → passed to `stage_downtime_local` only (H13) |
| Output | `rpt.oee_shift` → `abfss://curated@nwhmfglake.dfs.core.windows.net/manufacturing/oee_shift` | same path; parquet, **full overwrite** (≡ `TRUNCATE` + `INSERT` + Copy) |

## Grain

One row per `(plant_id, line_id, production_day, shift_code)` — the
`stg.planned_time` grain (`dim.shift_calendar` PK × `dim.line`), restricted by
the inner join to shifts with production. Reconcile keys in
`tools/reconcile_config.yaml` match.

## Inputs (DDL from `legacy/sql/schema/` and proc `CREATE TABLE`s)

| table | columns used | join |
|---|---|---|
| `dim.shift_calendar` | `plant_id CHAR(5)`, `shift_code VARCHAR(4)`, `production_day DATE`, `start_utc`/`end_utc DATETIME2(0)`; PK `(plant_id, shift_code, production_day)` | driving table |
| `dim.line` | `line_id VARCHAR(12)` PK, `plant_id CHAR(5)` | **inner** on `plant_id` (fan-out: every line × every plant shift) |
| `stg.downtime_shift_seg` | `plant_id CHAR(5)`, `line_id`, `production_day DATE`, `shift_code`, `planned_flag BIT`, `seg_start_local`/`seg_end_local DATETIME2(0)` | **left** on the 4 grain keys |
| `stg.production_local` | `plant_id`, `line_id`, `production_day DATE`, `shift_code`, `sku_id`, `bucket_minutes INT`, `total_units INT`, `good_units INT` | — |
| `dim.sku` | `sku_id VARCHAR(10)` PK, `ideal_units_per_min DECIMAL(9,2)` | **inner** on `sku_id` (in the `prod` CTE) |

## Output columns (`rpt.sql` / MANIFEST)

| target column | legacy type → parquet | derivation |
|---|---|---|
| `plant_id` | `CHAR(5)` → `string` | `dim.shift_calendar.plant_id`, **trimmed** |
| `line_id` | `VARCHAR(12)` → `string` | `dim.line.line_id` |
| `production_day` | `DATE` → `date` | `dim.shift_calendar.production_day` |
| `shift_code` | `VARCHAR(4)` → `string` | `dim.shift_calendar.shift_code` |
| `planned_min` | `INT` → `int` | `stg.planned_time.shift_minutes` = `DATEDIFF(MINUTE, start_utc, end_utc)` (**not** shift minus planned downtime, see Q1) |
| `unplanned_dt_min` | `INT` → `int` | `ISNULL(SUM(DATEDIFF(MINUTE, seg_start_local, seg_end_local)) WHERE planned_flag = 0, 0)` |
| `availability` | `DECIMAL(9,4) NULL` → `decimal(9,4)` | `ROUND(CAST(shift_min − unplanned AS DECIMAL(18,4)) / NULLIF(shift_min, 0), 4)` |
| `performance` | `DECIMAL(9,4) NULL` → `decimal(9,4)` | `ROUND(CAST(total AS DECIMAL(18,4)) / NULLIF(SUM(ideal_units_per_min × bucket_minutes), 0), 4)` |
| `quality` | `DECIMAL(9,4) NULL` → `decimal(9,4)` | `ROUND(CAST(good AS DECIMAL(18,4)) / NULLIF(total, 0), 4)` |
| `oee` | `DECIMAL(9,4) NULL` → `decimal(9,4)` | `ROUND(A_raw * total / NULLIF(ideal,0) * good / NULLIF(total,0), 4)`; the prod extract equals `ROUND(exact ratio, 4)` (H9) |

## Stages

### 0. Upstream (imported)
`plants_with_iana(dim.plant)` → `build_shift_calendar(plants, dim.shift_pattern, dim.calendar)`;
`stage_downtime_local(mes.downtime_event, dim.line, plants, as_of)` → `split_by_shift(local, shift_calendar)` (`stg.downtime_shift_seg`);
`stg_production_local(mes.production_count, dim.line, plants, shift_calendar)` (`stg.production_local`).

### 1. `mes.usp_calc_planned_time` → `stg_planned_time(shift_calendar, line, seg)`
`dim.shift_calendar ⋈ dim.line` (inner, `plant_id`) ⟕ `stg.downtime_shift_seg` (4 keys);
`GROUP BY plant_id, line_id, production_day, shift_code, start_utc, end_utc`.
Output = `stg.planned_time` DDL: `plant_id, line_id, production_day, shift_code, shift_minutes, planned_dt_min, unplanned_dt_min` (all `INT NOT NULL`). No logging / TRY-CATCH in legacy.

### 2. `rpt.usp_rpt_oee_shift` → `rpt_oee_shift(planned_time, production_local, sku)`
`prod` CTE = `stg.production_local ⋈ dim.sku` (inner) grouped to the grain; `stg.planned_time ⋈ prod` (inner) `WHERE production_day >= '2025-10-20'`; ratio expressions above; INSERT casts to `DECIMAL(9,4)`. `etl.usp_log_run` (open / close with `@@ROWCOUNT` / `FAILED` + `THROW`) → job logging + non-zero exit.

## SQL Server semantic hazards

| # | hazard (legacy code) | PySpark equivalent |
|---|---|---|
| H1 | `shift_minutes = DATEDIFF(MINUTE, sc.start_utc, sc.end_utc)` on **UTC** bounds: the fall-back night shift is 540 (US) / 780 (UK 12 h), a spring-forward night 420 | shared `datediff_minute()` on the UTC calendar columns |
| H2 | planned / unplanned downtime = `DATEDIFF(MINUTE, seg_start_local, seg_end_local)` on **local wall-clock**: across fall-back it is elapsed − 60, and a segment wholly inside the repeated hour can be **negative** (01:30 CDT → 01:10 CST = −20) | `datediff_minute()` on the imported `seg_start_local`/`seg_end_local`; negative values kept (parity) |
| H3 | `DATEDIFF` counts minute boundaries crossed (`10:00:59 → 10:01:00` = 1) | `datediff_minute()` (`floor(epoch/60)` difference) |
| H4 | `ISNULL(SUM(CASE WHEN planned_flag = 1 THEN … END), 0)`; `LEFT JOIN` — a shift with no segments gets 0 / 0, not NULL; only `planned_flag = 0` reduces availability | `coalesce(sum(when(flag, m)), 0)` / `coalesce(sum(when(~flag, m)), 0)` |
| H5 | `dim.shift_calendar ⋈ dim.line` inner join on `plant_id`: calendar rows of a plant with no lines are dropped; segments whose 4-key has no calendar × line row vanish through the left join | same joins; both drops counted + sample keys logged |
| H6 | `stg.planned_time ⋈ prod` **inner**: shifts with no production rows are dropped from the report; production rows with no planned-time row are dropped | same; both sides counted + sample keys logged |
| H7 | hard-coded `WHERE pt.production_day >= '2025-10-20'` (string literal → `DATE`) | constant `LEGACY_MIN_PRODUCTION_DAY = date(2025, 10, 20)`; filtered count logged (parity; Q2) |
| H8 | `prod` CTE inner join `dim.sku` drops stage rows with an unknown SKU (their units never reach the report) | same; count + sample `sku_id`s logged |
| H9 | `ideal_units = SUM(DECIMAL(9,2) × INT)` (runtime-weighted, `DECIMAL(38,2)`). `oee` is the left-to-right chain `((((A/sm)·T)/I)·G)/T`. SQL Server's documented result-type rules would round its intermediates to 6 dp, `(29,15)→(38,9)→(38,7)→(38,6)→(38,6)`. **The prod extract does not show that rounding.** All 1 792 snapshot rows equal `ROUND(exact rational value, 4)` for all four ratios. The 6-dp chain model mismatches 4 `oee` rows (rounding intermediates) or 1 row (truncating them); see Divergences | every ratio is computed by `_round_ratio_half_away(num, den)` on exact integers: `sign · ((2·\|n\|·10⁴ + \|d\|) div (2·\|d\|)) · 0.0001` in `DECIMAL(38,0)` with Spark's exact integral `div`, so no intermediate rounding happens. Inputs: A = `(sm−udt)/sm`; P = `100·T / (100·I)`; Q = `G/T`; OEE = `100·(sm−udt)·G / (sm · 100·I)`, NULL when T = 0 (T cancels algebraically) |
| H10 | `ROUND(decimal, 4)` rounds half away from zero (0.90625 → 0.9063, −0.03125 → −0.0313) | built into `_round_ratio_half_away`, using the magnitude plus a sign; there is no double, no `F.round`, and no 6-dp intermediate (that would double-round: exact 0.12344999… → 0.123450 → 0.1235) |
| H11 | `NULLIF(den, 0)` → NULL (not 0, not an error). `quality`/`oee` are NULL when total = 0, `performance`/`oee` when ideal = 0, `availability`/`oee` when shift_minutes = 0. `performance` = 0.0000 when total = 0 and ideal > 0 | `_round_ratio_half_away` returns NULL when den is 0 or NULL. `oee` is also NULL when total = 0 |
| H12 | `INSERT` into `DECIMAL(9,4)`: a value ≥ 100000 raises *Arithmetic overflow* (proc `THROW`s); `SUM(int)` returns INT and overflows with an error | explicit checks raise `OverflowError` (Spark non-ANSI casts would silently return NULL) |
| H13 | `AsOfUtc` reaches OEE only through `stg.downtime_shift_seg`: events with `start_utc >= AsOfUtc` excluded, open events capped at `AsOfUtc`; production is **not** cut off | `--as-of-utc` passed to the imported `stage_downtime_local` only |
| H14 | `CHAR(5)` `plant_id` space-padded | trimmed (upstream helpers + `dim.line`) before joins and in the output |
| H15 | time zones: `AT TIME ZONE` with Windows names (in the upstream calendar / segment stages) | imported upstream helpers (`mfg_lake.common.tz`, fail on unmapped); no new conversion in this job |

## Open questions / reviewer notes

- **Q1** `mes.usp_calc_planned_time` header says *planned production minutes = shift length minus PLANNED downtime*, and `rpt.usp_rpt_oee_shift` history says *2021-02 exclude planned dt from A*. The code instead reports `planned_min = shift_minutes` and uses `shift_minutes` as the availability denominator; `planned_dt_min` is computed but never used. The snapshot agrees with the code (e.g. 480 / 540 / 720 / 780). **Parity kept**; proposed fix (needs business approval): `planned_min = shift_minutes − planned_dt_min` and `availability = (planned_min − unplanned) / planned_min`.
- **Q2** H7 hard-codes the first day of the current reporting window; once the window moves past 2025-10-20 it no longer filters anything. Parity kept; proposed fix: drive it from the calendar window / `AsOfUtc`.
- **Q3** H2 negative / short local-time downtime across fall-back inflates availability on that night (legacy bug, parity kept).

## Seed-data coverage (`make seed`, AsOfUtc 2025-11-17 00:00:00)

| measure | count |
|---|---|
| `mes.downtime_event` rows → staged (`start_utc < AsOfUtc`) / capped open events | 455 → 450 / 1 |
| `stg.downtime_shift_seg` segments (planned / unplanned) | 520 (2 / 518) |
| segments where local `DATEDIFF` ≠ UTC elapsed (fall-back night, H2): 3 US S3 2025-11-01, 1 UK N 2025-10-25; each is −60 min, all unplanned | 4 |
| segments with negative local minutes (wholly inside the repeated hour) | 0 |
| shift_minutes distribution (H1): 480 / 540 / 720 / 780 | 333 / 3 / 111 / 1 calendar rows |
| `stg.planned_time` rows / with unplanned > 0 / with planned > 0 | 1 792 / 488 / 2 |
| calendar rows without a line, orphan segments, planned_time without production, production without planned_time, unknown SKUs, rows before 2025-10-20 | 0 each |
| `stg.production_local` rows / 75-minute (fall-back) buckets | 61 336 / 15 |
| report rows with total = 0 (`quality`, `oee` NULL) / ideal = 0 / shift_minutes = 0 | 2 / 0 / 0 |
| exact-half `availability` before rounding (x.xxxx5) | 64 |
| rows where the 6-dp chain model ≠ exact-ratio rounding (H9) | 4 |

Not exercised by the seed (unit tests only): negative local downtime, plants without lines, orphan segments, shifts without production (and the reverse), unknown SKUs, rows before the H7 date, ideal = 0, shift_minutes = 0, DECIMAL(9,4)/INT overflow, negative availability, spring-forward (420-minute) shifts.

## Implementation

`stg_planned_time()` implements `mes.usp_calc_planned_time`. `rpt_oee_shift()` implements the `prod` CTE plus `rpt.usp_rpt_oee_shift`. `build_report()` wires the imported upstream stages (`plants_with_iana`, `build_shift_calendar`, `stage_downtime_local` with the cutoff, `split_by_shift`, `stg_production_local`) and logs the count for each stage. `run()` full-overwrites the curated parquet. No shared files were changed apart from one row in `lakehouse/adf/README.md`.

## Hazard → covering test (`tests/test_oee_shift.py`)

| Hazard | Covering tests |
|---|---|
| H1 | `test_shift_minutes_use_utc_bounds` (480 / 540 US fall-back / 780 UK fall-back / 420 spring-forward) |
| H2 | `test_fall_back_downtime_uses_local_wall_clock`, `test_segment_inside_repeated_hour_is_negative`, `test_full_seed_planned_time_utc_vs_legacy_local_split` (literal local-time split port, `exceptAll` both ways on the full seed) |
| H3 | `test_downtime_minutes_count_minute_boundaries` |
| H4 | `test_planned_and_unplanned_split_and_isnull` |
| H5 | `test_calendar_line_join_drops_plant_without_lines_logged` |
| H6 | `test_shift_without_production_dropped_and_logged` |
| H7 | `test_hard_coded_min_production_day` |
| H8 | `test_unknown_sku_dropped_from_prod_and_logged` |
| H9 | `test_performance_is_runtime_weighted_decimal`, `test_oee_rounds_exact_ratio`, `test_seed_snapshot_oee_rounding_edge` (the 4 divergent seed rows), `test_ratios_match_exact_fraction_oracle` (20 000-row `Fraction` oracle), `test_double_rounding_regression` |
| H10 | `test_round_half_away_from_zero`, `test_negative_availability_rounds_away_from_zero` |
| H11 | `test_zero_denominators_are_null` |
| H12 | `test_decimal_9_4_overflow_raises`, `test_int_sum_overflow_raises` |
| H13 | `test_as_of_cutoff_flows_through_downtime_stage_only`; adversarial check A |
| H14 | `test_padded_plant_ids_trimmed` |
| H15 | upstream tests in `test_line_downtime_daily.py` / `test_daily_production.py` (imported helpers) |
| output contract | `test_report_schema_matches_legacy_types`, `test_job_has_no_wall_clock_calls` |

## Dropped / filtered row logging

| Join / filter | Seed-run result |
|---|---|
| `mes.downtime_event` → `stg.downtime_local` (cutoff, `dim.line`/plant joins) | 450 of 455 kept, 1 capped at AsOfUtc |
| `daily_production` stage `dim.line` / `dim.plant` / `dim.shift_calendar` inner joins | 0 / 0 / 0 dropped |
| `dim.shift_calendar ⋈ dim.line` (plant without lines) | 0 dropped |
| segments with no calendar × line row (left join) | 0 dropped |
| `dim.sku` inner join (`prod` CTE) | 0 dropped |
| `production_day >= '2025-10-20'` | 0 filtered |
| `planned_time ⋈ prod`: planned rows without production / production without planned rows | 0 / 0 dropped |

## Adversarial checks (`docs/migration/evidence/oee_shift/adversarial.log`)

| Check | Result |
|---|---|
| A: `--as-of-utc "2025-11-10 00:00:00"` into `adv_asof` | expected FAIL, exit 2, 11/16 controls; 369 of 455 events kept; `unplanned_dt_min` / `availability` / `oee` each have 92 mismatches (e.g. `PLT01\|PLT01-L1\|2025-11-11\|S1 unplanned_dt_min: lake='0' legacy='141'`) |
| B: canonical runs into `adv_r1` and `adv_r2` | PASS: sorted CSV bytes identical (113 124 bytes), both SHA-256 `36214346b8c090e3a518a3daff86a95f8841a74ca581518c53e35d55ceca5029` |
| C: one `oee` value +0.0001 in `adv_corrupt` | expected FAIL, exit 2, 15/16 controls; exactly one mismatch, `PLT01\|PLT01-L1\|2025-10-20\|S1 oee: lake='0.8359' legacy='0.8358'` |

## Reconciliation

`make ci` (ns=ci) result: line_downtime_daily 13/13, daily_production 13/13, oee_shift **16/16 controls PASS**.

```text
PASS  row_count                         lake=1792 legacy=1792
PASS  key_set                           missing=0 extra=0
PASS  col[plant_id].mismatches          n=0
PASS  col[line_id].mismatches           n=0
PASS  col[production_day].mismatches    n=0
PASS  col[shift_code].mismatches        n=0
PASS  col[planned_min].checksum         lake=968640.0000 legacy=968640.0000
PASS  col[planned_min].mismatches       n=0
PASS  col[unplanned_dt_min].checksum    lake=35719.0000 legacy=35719.0000
PASS  col[unplanned_dt_min].mismatches  n=0
PASS  col[availability].checksum        lake=1726.4051 legacy=1726.4051
PASS  col[availability].mismatches      n=0
PASS  col[performance].checksum         lake=1574.5355 legacy=1574.5355
PASS  col[performance].mismatches       n=0
PASS  col[quality].mismatches           n=0
PASS  col[oee].mismatches               n=0
```

The reconcile harness needed no change: `decimal(9,4)` and NULL handling are already covered by `_num` / `_norm`.

## Divergences found and fixed

**H9 `oee` rounding.** The first implementation used SQL Server's documented decimal result-type rules, with the left-to-right chain rounded to `(38,6)` intermediates. Result: `FAIL col[oee].mismatches n=4` (15/16). Example: `PLT03|PLT03-L1|2025-10-23|S3 lake='0.8655' legacy='0.8654'`. Here the exact ratio is 78929/91200 = 0.86544956…, so the 6-dp intermediate 0.865450 rounds up. I replayed all 1 792 rows:

- rounding intermediates at 6 dp → 4 mismatches
- truncating them → 1 mismatch
- `ROUND(exact ratio, 4)` → 0 mismatches, for all four ratios

The job now uses exact integer rounding (`_round_ratio_half_away`) → 16/16 PASS. The four rows are pinned in `test_seed_snapshot_oee_rounding_edge`.

## Deviations from legacy: none

Legacy bugs are kept (Q1–Q3 above); proposed fixes need business approval.
