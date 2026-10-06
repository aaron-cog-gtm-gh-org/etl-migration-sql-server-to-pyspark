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
| `oee` | `DECIMAL(9,4) NULL` → `decimal(9,4)` | `ROUND(A_raw * total / NULLIF(ideal,0) * good / NULLIF(total,0), 4)` evaluated **left to right** (H9) |

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
| H9 | `ideal_units = SUM(DECIMAL(9,2) × INT)` → `DECIMAL(38,2)`; the ratios are DECIMAL divisions with SQL Server result-type rules: A, Q `DECIMAL(29,15)`, P `DECIMAL(38,22)`; `oee` is the **left-to-right chain** `((((A/sm)·T)/I)·G)/T` with intermediate types `(29,15)→(38,9)→(38,7)→(38,6)→(38,6)` — **not** `A×P×Q` and not the product of rounded components (e.g. sm 720, udt 58, T 39313, G 1833, I 38522.33 → legacy 0.0438, exact rational 0.0437) | identical casts (`decimal(18,4)`, INT → `decimal(10,0)`, ideal → `decimal(38,2)`) and the same operator order; Spark 3.5's `allowPrecisionLoss` rules yield the same intermediate types (checked against a Python `Decimal` oracle of the SQL Server rules on 20 002 rows: 0 mismatches) |
| H10 | `ROUND(decimal, 4)` rounds half away from zero (0.90625 → 0.9063) | `F.round` on decimal (HALF_UP); never on double |
| H11 | `NULLIF(den, 0)` → NULL (not 0, not an error): `quality`/`oee` NULL when total = 0, `performance`/`oee` NULL when ideal = 0, `availability`/`oee` NULL when shift_minutes = 0; `performance` = 0.0000 when total = 0 and ideal > 0 | `when(den != 0, den)` |
| H12 | `INSERT` into `DECIMAL(9,4)`: a value ≥ 100000 raises *Arithmetic overflow* (proc `THROW`s); `SUM(int)` returns INT and overflows with an error | explicit checks raise `OverflowError` (Spark non-ANSI casts would silently return NULL) |
| H13 | `AsOfUtc` reaches OEE only through `stg.downtime_shift_seg`: events with `start_utc >= AsOfUtc` excluded, open events capped at `AsOfUtc`; production is **not** cut off | `--as-of-utc` passed to the imported `stage_downtime_local` only |
| H14 | `CHAR(5)` `plant_id` space-padded | trimmed (upstream helpers + `dim.line`) before joins and in the output |
| H15 | time zones: `AT TIME ZONE` with Windows names (in the upstream calendar / segment stages) | imported upstream helpers (`mfg_lake.common.tz`, fail on unmapped); no new conversion in this job |

## Open questions / reviewer notes

- **Q1** `mes.usp_calc_planned_time` header says *planned production minutes = shift length minus PLANNED downtime*, and `rpt.usp_rpt_oee_shift` history says *2021-02 exclude planned dt from A*. The code instead reports `planned_min = shift_minutes` and uses `shift_minutes` as the availability denominator; `planned_dt_min` is computed but never used. The snapshot agrees with the code (e.g. 480 / 540 / 720 / 780). **Parity kept**; proposed fix (needs business approval): `planned_min = shift_minutes − planned_dt_min` and `availability = (planned_min − unplanned) / planned_min`.
- **Q2** H7 hard-codes the first day of the current reporting window; once the window moves past 2025-10-20 it no longer filters anything. Parity kept; proposed fix: drive it from the calendar window / `AsOfUtc`.
- **Q3** H2 negative / short local-time downtime across fall-back inflates availability on that night (legacy bug, parity kept).
