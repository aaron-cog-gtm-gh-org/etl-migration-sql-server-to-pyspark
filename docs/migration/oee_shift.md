# PL_OEE -> `mfg_lake.jobs.oee_shift` (KAN-7)

Legacy: `legacy/adf/pipeline/PL_OEE.json`, including
`mes.usp_calc_planned_time` and `rpt.usp_rpt_oee_shift`. Target:
`lakehouse/src/mfg_lake/jobs/oee_shift.py`,
`lakehouse/adf/pipeline/PL_OEE.json`, curated report
`abfss://curated@kcmfglake.dfs.core.windows.net/manufacturing/oee_shift`.
Snapshot: `legacy_snapshots/rpt.oee_shift.csv` (1792 rows; read-only).

## Pipeline → job mapping

| legacy activity / procedure | PySpark function |
|---|---|
| `SP_CalcPlannedTime` → `mes.usp_calc_planned_time` | `calc_planned_time(shift_calendar, line, downtime_shift_seg)` |
| `SP_RptOeeShift` → `rpt.usp_rpt_oee_shift` | `rpt_oee_shift(planned_time, production_local, sku)` |
| `COPY_rpt_oee_shift_to_curated` → `DS_ADLS_Curated_Parquet(report=oee_shift)` | `run()` → `write_curated(..., mode="overwrite")` |
| `PL_Master` shift-calendar stage | imported `build_shift_calendar()` |
| `SP_StgDowntimeLocal` / `SP_SplitDowntimeByShift` | imported `stage_downtime_local()` / `split_downtime_by_shift()` |
| `SP_StgProductionCounts` | imported `stage_production_local()` |

The report job runs the same dependencies in-process; upstream staging
outputs are not persisted as separate curated reports.

## Grain and output schema

One row per `(plant_id, line_id, production_day, shift_code)`.
Reconciliation keys are the same four columns.

| column | SQL Server contract | lakehouse type |
|---|---|---|
| `plant_id` | `CHAR(5) NOT NULL` | string |
| `line_id` | `VARCHAR(12) NOT NULL` | string |
| `production_day` | `DATE NOT NULL` | date |
| `shift_code` | `VARCHAR(4) NOT NULL` | string |
| `planned_min` | `INT NOT NULL` | integer |
| `unplanned_dt_min` | `INT NOT NULL` | integer |
| `availability` | `DECIMAL(9,4) NULL` | decimal(9,4), nullable |
| `performance` | `DECIMAL(9,4) NULL` | decimal(9,4), nullable |
| `quality` | `DECIMAL(9,4) NULL` | decimal(9,4), nullable |
| `oee` | `DECIMAL(9,4) NULL` | decimal(9,4), nullable |

Output order is exactly the DDL order above.

## Imported upstream stages

KAN-6's shift-calendar and production functions are imported, not copied:

| function | signature |
|---|---|
| `daily_production.build_shift_calendar` | `(plant: DataFrame, shift_pattern: DataFrame, calendar: DataFrame) -> DataFrame` |
| `daily_production.stage_production_local` | `(production_count: DataFrame, line: DataFrame, plant: DataFrame, shift_calendar: DataFrame) -> DataFrame` |

KAN-8's local downtime functions are imported, not copied:

| function | signature |
|---|---|
| `line_downtime_daily.stage_downtime_local` | `(downtime_event: DataFrame, line: DataFrame, plant: DataFrame, as_of_utc) -> DataFrame` |
| `line_downtime_daily.split_downtime_by_shift` | `(downtime_local: DataFrame, plant: DataFrame, shift_calendar: DataFrame) -> DataFrame` |
| `line_downtime_daily.datediff_minute` | `(start, end) -> Column` |

The cutoff is applied only by the imported downtime staging function.

## Hazards and test coverage

| legacy semantics / hazard | PySpark equivalent | test |
|---|---|---|
| `shift_minutes` is `DATEDIFF(MINUTE, start_utc, end_utc)` | imported `datediff_minute` on UTC calendar bounds | `test_shift_minutes_use_utc_bounds` |
| downtime is `DATEDIFF(MINUTE, seg_start_local, seg_end_local)` and counts minute boundaries | imported `datediff_minute` on local segment bounds | `test_segment_minutes_use_local_boundaries` |
| DST shifts have different elapsed UTC durations and legacy local-minute behavior | use UTC calendar bounds for shift length; do not recompute from local timestamps | `test_dst_shift_minutes_match_utc_duration` |
| Shift durations across both zones and transitions: 9h/7h for Central 3x8, 13h/11h for London 2x12 | preserved through `build_shift_calendar` plus UTC `DATEDIFF` | `test_dst_shift_minutes_match_utc_duration` |
| `planned_flag=1` and `planned_flag=0` are summed separately | separate planned and unplanned minute sums | `test_planned_and_unplanned_downtime_are_separate` |
| The proc names `planned_min` as planned production time, but actually inserts `shift_minutes` | `planned_min = shift_minutes`; planned downtime is not subtracted | `test_planned_and_unplanned_downtime_are_separate` |
| Availability subtracts only unplanned downtime | `(shift_minutes - unplanned_dt_min) / shift_minutes` | `test_planned_and_unplanned_downtime_are_separate` |
| Ideal output is runtime-weighted: `SUM(ideal_units_per_min * bucket_minutes)` | aggregate the exact products before dividing total units | `test_runtime_weighted_ideal` |
| `NULLIF` guards each zero denominator | nullable ratios when `shift_minutes`, ideal units, or total units is zero | `test_nullif_guards` |
| SQL `ROUND(x,4)` rounds ties half away from zero, including negative values | exact integer ratio rounding, then `DECIMAL(9,4)` cast | `test_round_half_away_from_zero` |
| Rounded component multiplication can differ from a single exact OEE ratio | calculate OEE from `(s-u)*T*G / (s*I*T)` and round once | `test_oee_rounds_once_on_exact_product`, `test_snapshot_oee_rounding_regression` |
| Report hard-codes `production_day >= '2025-10-20'` | `PRODUCTION_DAY_FLOOR = date(2025, 10, 20)` | `test_production_day_floor` |
| `calc_planned_time` inner-joins calendar to lines by plant | inner join; a calendar row for a plant with no line creates no planned row | `test_calendar_without_line_is_dropped` |
| OEE inner-joins production to `dim.sku` | unknown SKU production is dropped | `test_unknown_sku_is_dropped` |
| OEE inner-joins `planned_time` to production on the full four-part grain | planned-only and production-only groups are both dropped | `test_planned_time_and_production_inner_join` |
| Calendar-to-segment join is a left join; absent sums use `ISNULL(...,0)` | left join segments and coalesce both downtime sums to zero | `test_shift_without_segments_has_zero_downtime` |
| Duplicate segment rows and production buckets participate repeatedly in `SUM`; no deduplication | do not distinct either input | `test_duplicate_segments_and_buckets_are_summed` |
| Unknown line / plant / SKU keys fail the legacy inner join | preserve the imported stage and report inner joins | `test_unknown_line_is_dropped_by_imported_production_stage`, `test_unknown_sku_is_dropped` |
| SQL `SUM(INT)` and insertion into `INT` overflow with an error | aggregate in long, check signed 32-bit bounds, then cast | `test_integer_overflow_raises` |

## `AsOfUtc`

Neither OEE procedure receives `AsOfUtc`. The value affects OEE only because
the imported KAN-8 `stage_downtime_local` applies `start_utc < as_of` and
caps open events at `as_of`. The OEE job passes `as_of_utc` only to that
function; it is not a production, calendar, or report filter.

| cutoff | changed snapshot values |
|---|---:|
| `2025-11-10 00:00:00` | 92 |
| `2025-11-16 12:00:00` | 9 |
| `2025-11-20 00:00:00` | 7 |
| `2025-11-17 00:00:01` | 0 |

Thus an earlier cutoff is expected to FAIL reconcile. This verified
data-flow behavior contradicts the migration playbook table, which says
`PL_OEE` should be IDENTICAL for earlier cutoffs.

## Ticket acceptance criteria (KAN-7)

KAN-7 also lists "SQL Server behaviours the migration must preserve". One
of them conflicts with the legacy procs:

| ticket behaviour | status | reason |
|---|---|---|
| Planned minutes = shift length minus PLANNED downtime | **not met, by design** | `mes.usp_calc_planned_time` computes `planned_dt_min`, but `rpt.usp_rpt_oee_shift` never uses it. It writes `planned_min = shift_minutes`, and availability is `(shift_minutes - unplanned_dt_min) / shift_minutes`. The prod extract agrees: PLT02-L1 2025-10-30 S1 has 90 planned downtime minutes, and the snapshot shows `planned_min=480`, `availability=1.0000`. Exact parity wins. |
| Runtime-weighted ideal rate | met | `SUM(ideal_units_per_min * bucket_minutes)` |
| Each ratio DECIMAL(9,4) with `NULLIF` guards | met | exact ratio, rounded once, half away from zero; NULL on a zero denominator |
| DST-length shifts | met | UTC `DATEDIFF` on calendar bounds (540/420 Central, 780/660 London) |

| AC | status | evidence / note |
|---|---|---|
| 1. `mfg_lake.jobs.oee_shift` with `--ns` / `--as-of-utc`, writes curated `oee_shift` | met | `test_cli_requires_and_parses_ns_and_as_of_utc`, `make run JOB=oee_shift` |
| 2. Tests before the job, covering the listed behaviours | met, except the planned-minutes behaviour (not met, by design; above) | tests commit precedes the job commit. `test_planned_and_unplanned_downtime_are_separate` pins the legacy behaviour. |
| 3. `make reconcile REPORT=oee_shift` passes: 1792 rows, 0/0 keys, 0 mismatches | met | see Validation evidence |
| 4. Earlier cutoff and a corrupted value fail reconcile; rerun is identical | met | The earlier cutoff fails only through the imported KAN-8 downtime stage (see `AsOfUtc`). It contradicts the playbook table, not the ticket. |
| 5. `PL_OEE.json`: single Spark activity; name, `AsOfUtc` and `PL_Master` dependencies unchanged | met | `lakehouse/adf/pipeline/PL_OEE.json`. Legacy PL_OEE has no retry policy, and none is added. |
| 6. Mapping doc with deviations and sign-off | met | this document |
| 7. `legacy/` and `legacy_snapshots/` unchanged; PR with green CI | met locally (checksums in the recorded run); CI pending | |

## Deviations and data handling

The source DDL declares all `mes.production_count` columns `NOT NULL`.
`stage_production_local` drops and the job counts rows with NULLs in
`line_id`, `bucket_start_utc`, `bucket_end_utc`, `sku_id`, `total_units`, or
`good_units`; `end_utc` is the only nullable downtime-event source column
and remains a valid open event. `stage_downtime_local` drops and counts
NULLs in downtime `event_id`, `line_id`, `start_utc`, `reason_code`, and
`planned_flag`. These invalid rows cannot exist in legacy source tables.
Unknown dimension keys are removed by the same inner joins as legacy.

Aggregates use wider Spark integers, with explicit signed `INT` range
checks to preserve SQL Server overflow failure rather than silently wrap.
No deduplication is performed: legacy sums duplicate segment and bucket
rows. `planned_min` intentionally remains the shift duration despite the
procedure's misleading comment and the ticket's AC1 definition.

## Coverage gaps

- No live SQL Server execution is available; the oracle is an independent
  Python port of the checked-in proc semantics.
- ADF JSON is reviewed locally, not deployed; Databricks and ADLS are not
  exercised by local validation.
- Canonical production data contains the reported DST cases, while the
  full transition matrix is covered by unit tests.

## Validation evidence

_To be filled after the validation and fuzz runs._

- `make test`: _pending_
- `make run JOB=oee_shift NS=dev` + reconcile: _pending_
- `make ci`: _pending_
- fuzz runs: _pending_
- canonical bcp compare: _pending_

## Sign-off

| role | name | date | decision |
|---|---|---|---|
| Migration engineer | Devin (KAN-7) | | implemented, validation pending |
| MES reporting owner | | | |
| Data platform | | | |

Stacked on #6 → #5; retarget after those merge. Supersedes #3.
