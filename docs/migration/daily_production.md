# Source-to-target mapping: `rpt.daily_production` → curated `daily_production`

| | Legacy | Target |
|---|---|---|
| Orchestration | ADF `PL_Daily_Production` (`legacy/adf/pipeline/`): `SP_StgProductionCounts` → `SP_RptDailyProduction` → `COPY_rpt_daily_production_to_curated` | ADF `PL_Daily_Production` (`lakehouse/adf/pipeline/`): single `SPK_DailyProduction` (DatabricksSparkPython) |
| Code | `mes.usp_stg_production_counts` (+ `dim.ufn_production_day`), `rpt.usp_rpt_daily_production` (+ `dim.shift_calendar` from `dim.usp_refresh_shift_calendar` in `PL_Master`) | `lakehouse/src/mfg_lake/jobs/daily_production.py` |
| Parameter | `AsOfUtc` (pipeline parameter; **not passed to either proc**) | `--as-of-utc` (validated with `parse_as_of_utc`, logged, otherwise unused — see hazard H10) |
| Output | `rpt.daily_production` → `abfss://curated@nwhmfglake.dfs.core.windows.net/manufacturing/daily_production` | same path; parquet, **full overwrite** (≡ `TRUNCATE`+`INSERT`+Copy) |
| Downstream | `stg.production_local` is read by `rpt.usp_rpt_oee_shift` (`PL_OEE`) and `rpt.usp_rpt_scrap_yield_weekly` (`PL_Scrap_Yield`) | `mfg_lake.jobs.daily_production.stg_production_local()` — import it, don't re-derive |

## Grain

One row per `(plant_id, line_id, production_day, sku_id)` (legacy `GROUP BY` also has `k.pack_size`, which is functionally dependent on the `dim.sku` PK `sku_id`). Reconcile keys in `tools/reconcile_config.yaml` match.

## Inputs (DDL from `legacy/sql/schema/`)

| table | columns used | notes |
|---|---|---|
| `mes.production_count` | `line_id VARCHAR(12)`, `bucket_start_utc DATETIME2(0)`, `bucket_end_utc DATETIME2(0)`, `sku_id VARCHAR(10)`, `total_units INT`, `good_units INT` | 15-min count buckets, UTC |
| `dim.line` | `line_id VARCHAR(12)` PK, `plant_id CHAR(5)` | inner join |
| `dim.plant` | `plant_id CHAR(5)` PK, `tz_name NVARCHAR(64)` (Windows name) | inner join |
| `dim.shift_calendar` | `plant_id CHAR(5)`, `shift_code VARCHAR(4)`, `start_utc`/`end_utc DATETIME2(0)` | not a raw feed: rebuilt from `dim.plant × dim.shift_pattern × dim.calendar` by the shared `build_shift_calendar()`; inner join |
| `dim.sku` | `sku_id VARCHAR(10)` PK, `pack_size INT` | inner join (report proc) |

## Output columns (`rpt.sql` / MANIFEST)

| target column | legacy type → parquet | derivation |
|---|---|---|
| `plant_id` | `CHAR(5)` → `string` | `dim.line.plant_id`, **trimmed** |
| `line_id` | `VARCHAR(12)` → `string` | `mes.production_count.line_id` |
| `production_day` | `DATE` → `date` | `dim.ufn_production_day(local bucket START)` |
| `sku_id` | `VARCHAR(10)` → `string` | `mes.production_count.sku_id` |
| `total_units` | `INT` → `int` | `SUM(total_units)` |
| `good_units` | `INT` → `int` | `SUM(good_units)` |
| `cases` | `INT NULL` → `int` | `SUM(good_units) / pack_size` — INT/INT integer division |
| `yield_pct` | `DECIMAL(9,2) NULL` → `decimal(9,2)` | `ROUND(CAST(SUM(good) AS DECIMAL(18,4)) / NULLIF(SUM(total),0) * 100, 2)` |

## Stages

### 1. `mes.usp_stg_production_counts` → `stg_production_local()` (exported stage)
`mes.production_count ⋈ dim.line ⋈ dim.plant ⋈ dim.shift_calendar` (all **inner**); shift match `bucket_start_utc >= sc.start_utc AND bucket_start_utc < sc.end_utc` (half-open, UTC). No `@AsOfUtc`, no filter.

Output schema (legacy `stg.production_local` DDL, same column order):

| column | type |
|---|---|
| `line_id` | string |
| `plant_id` | string (trimmed CHAR(5)) |
| `sku_id` | string |
| `production_day` | date |
| `shift_code` | string |
| `bucket_minutes` | int — `DATEDIFF(MINUTE, bucket_start_utc, bucket_end_utc)` on **UTC** values |
| `total_units` | int |
| `good_units` | int |

### 2. `rpt.usp_rpt_daily_production` → `rpt_daily_production()`
`stg.production_local ⋈ dim.sku` (inner), `GROUP BY` grain, aggregates above, final casts to legacy types. `etl.usp_log_run` (open row / close with `@@ROWCOUNT` / `FAILED` + `THROW`) is replaced by job logging and a non-zero exit on error.

## SQL Server semantic hazards

| # | hazard (legacy code) | PySpark equivalent |
|---|---|---|
| H1 | `bucket_start_utc AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name` with **Windows** tz names | `mfg_lake.common.tz` Windows→IANA map; `from_utc_timestamp`; unmapped name fails the job. UTC→local is never ambiguous, so the fall-back fold (two UTC instants → same local 01:xx) needs no rule here |
| H2 | `CAST(... AS DATETIME2(0))` rounding | source columns are already `DATETIME2(0)` and the seed has 0 sub-second values; whole-second `date_trunc` (shared helper) is exact |
| H3 | `dim.ufn_production_day`: `CAST(DATEADD(HOUR, -6, CAST(@local_dt AS DATETIME)) AS DATE)` | `to_date(local − 6 h)` on the naive local value; `DATETIME` cast is lossless for whole seconds. 06:00:00 local → same day, 05:59:59 → previous day |
| H4 | bucket attributed by **START** (2017-02 change) | production day and shift both from `bucket_start_utc`; a bucket straddling 06:00 local goes to the earlier day |
| H5 | `production_day` comes from the function, **not** `dim.shift_calendar.production_day`; `shift_code` comes from the calendar join | same; the two are never cross-checked (parity) |
| H6 | inner join to `dim.shift_calendar` on half-open `[start_utc, end_utc)` — buckets outside the calendar window are silently dropped; overlapping calendar rows would duplicate a bucket | same join; dropped count + sample keys logged; fan-out (>1 shift match) logged as WARNING but kept (parity) |
| H7 | `DATEDIFF(MINUTE, start_utc, end_utc)` counts minute boundaries, computed on **UTC** values | shared `datediff_minute()` on the UTC columns. A bucket 01:45 CDT → 02:00 CST is 75, not 15 |
| H8 | fall-back night shift in `dim.shift_calendar` (local → UTC via `AT TIME ZONE`) | shared `build_shift_calendar()` (moved to `mfg_lake.common.shift_calendar`), already reconciled by `line_downtime_daily` |
| H9 | inner joins `dim.line`, `dim.plant`, `dim.sku` drop rows silently | same joins; count + sample keys logged per join |
| H10 | **no `@AsOfUtc` anywhere** in either proc — buckets after the cutoff are included | parity: no cutoff filter. `--as-of-utc` is validated and logged only |
| H11 | `SUM(int)` returns INT (overflow → error); `SUM(good) / pack_size` is INT/INT **integer division** (truncates toward zero); `pack_size = 0` → divide-by-zero error, proc `THROW`s | sums checked against the INT range (raise on overflow); `div` (truncates toward zero); `pack_size = 0` on a joined row raises |
| H12 | `CAST(SUM(good) AS DECIMAL(18,4)) / NULLIF(SUM(total),0) * 100`, `ROUND(..., 2)` half away from zero, into `DECIMAL(9,2)` | same decimal types (`decimal(18,4)` / `decimal(10,0)` × `decimal(3,0)` → `decimal(33,15)`), `round` on decimal (HALF_UP = away from zero), cast `decimal(9,2)`; never on double. `NULLIF` → `yield_pct` NULL (not 0) when `SUM(total) = 0` |
| H13 | `CHAR(5)` `plant_id` is space-padded | trimmed before joins and in output |
| H14 | `VARCHAR` equality ignores trailing spaces (and the server collation may be case-insensitive) | keys trimmed; case-insensitive matching **not** implemented — seed keys all match exactly (open question below) |

## Seed-data coverage (`make seed`, AsOfUtc 2025-11-17 00:00:00)

| measure | count |
|---|---|
| `mes.production_count` rows | 61,336 |
| buckets with `bucket_start_utc >= AsOfUtc` (all on production day 2025-11-16; present in the snapshot) | 979 |
| buckets in the DST fall-back night window (US 2025-11-02 / UK 2025-10-26, local 00:00–03:00) | 180 |
| 75-minute buckets (cross a fall-back) | 15 |
| buckets starting exactly 06:00 local (production-day boundary) | 641 |
| zero-total buckets / zero-total report groups (`yield_pct` NULL) | 415 / 1 (`PLT06-L4`, 2025-11-04) |
| unmatched `line_id` / `sku_id`, sub-second timestamps, padded values | 0 / 0 / 0 / 0 |
| exact-half `yield_pct` cases (x.xx5) | 0 |

Not exercised by the seed (unit tests only): buckets outside the shift calendar, unmatched `dim.line`/`dim.plant`/`dim.sku` rows, `pack_size = 0`, INT overflow, exact-half rounding, a bucket straddling 06:00 local, negative unit counts.

## Open questions / reviewer notes

- H10: a cutoff earlier than the seed window must still PASS reconciliation because the legacy procs never read `AsOfUtc`. Applying a cutoff would drop the 979 post-cutoff buckets and break parity.
- H14: if the prod `MES_Reporting` collation is case-insensitive, legacy would join keys differing only in case; the job would drop them (and log it). Not seen in the seed.

## Implementation

`stg_production_local()` implements the reusable `stg.production_local` stage;
`rpt_daily_production()` performs the SKU join, overflow/divide checks, grouped
aggregates, integer cases, and decimal yield; `build_report()` wires the stages
and validates/logs the intentionally unused cutoff. `run()` full-overwrites the
curated parquet report. Time conversion and shift-calendar helpers now live in
`mfg_lake.common.timeconv` and `mfg_lake.common.shift_calendar`; the line
downtime module retains the prior helper names as imports/aliases.

## Hazard → covering test

| Hazard | Covering tests |
|---|---|
| H1 | `test_timezone_conversion_and_dst_per_plant`, `test_unmapped_plant_timezone_raises` |
| H2 | `test_bucket_timestamps_are_truncated_to_whole_seconds`, `test_full_seed_stage_count_and_totals` |
| H3 | `test_ufn_production_day` |
| H4 | `test_bucket_straddling_production_day_boundary_uses_start` |
| H5 | `test_timezone_conversion_and_dst_per_plant`, `test_fall_back_fold_buckets_both_match_prior_day_s3` |
| H6 | `test_shift_boundary_is_half_open`, `test_dropped_line_plant_and_shift_buckets_are_logged`, `test_shift_calendar_fanout_is_logged_and_kept` |
| H7 | `test_bucket_minutes_are_counted_on_utc_minute_boundaries` |
| H8 | `test_fall_back_fold_buckets_both_match_prior_day_s3`; unchanged `test_shift_calendar_fall_back_night_is_nine_hours` in `test_line_downtime_daily.py` |
| H9 | `test_dropped_line_plant_and_shift_buckets_are_logged`, `test_unmatched_line_and_sku_are_logged` |
| H10 | `test_as_of_is_validated_but_does_not_filter` |
| H11 | `test_cases_use_sum_then_integer_division_toward_zero`, `test_zero_pack_size_raises_sql_server_error`, `test_integer_sum_overflow_raises` |
| H12 | `test_yield_pct_decimal_rounding`, `test_zero_total_yield_is_null_and_cases_zero`, `test_decimal_column_and_date_key_match_snapshot_strings` |
| H13 | `test_stage_trims_char_padded_ids_and_has_legacy_schema` |
| H14 | `test_stage_trims_char_padded_ids_and_has_legacy_schema`; case-insensitive matching remains an open collation question |

## Dropped / filtered row logging

| Join / stage | Seed-run result |
|---|---|
| `dim.line` inner join | 0 buckets dropped |
| `dim.plant` inner join | 0 buckets dropped |
| `dim.shift_calendar` half-open UTC inner join | 0 buckets dropped |
| `dim.sku` inner join | 0 stage rows dropped |
| Source buckets → staged buckets | 61,336 → 61,336 |

## Adversarial checks

| Check | Result |
|---|---|
| `--as-of-utc "2025-11-10 00:00:00"` into `adv_asof` | PASS, 13/13 controls; all 61,336 source buckets remain staged |
| Canonical runs into `adv_r1` and `adv_r2` | PASS, sorted CSV bytes identical (40,687 bytes); both SHA-256 `93bdc9425b7d2b2f3100fce1027f7ae06f525c495511f9e2173fb5ffe989b677` |
| Increment one `good_units` in `adv_corrupt` | Expected FAIL; schema preserved; exactly one value changed (`238578` → `238579`) |

Corruption reconciliation retained row count and key-set PASS, with only the
`good_units` checksum and one value-mismatch control failing. The sample diff
was `key=PLT01|PLT01-L1|2025-10-20|SKU-TT18 col=good_units: lake='238579' legacy='238578'`.
Full run output is in `docs/migration/evidence/daily_production/adversarial.log`.

## Reconciliation

Canonical dev reconciliation: 13/13 controls PASS.

```text
PASS  row_count                       lake=672 legacy=672
PASS  key_set                         missing=0 extra=0
PASS  col[plant_id].mismatches        n=0
PASS  col[line_id].mismatches         n=0
PASS  col[production_day].mismatches  n=0
PASS  col[sku_id].mismatches          n=0
PASS  col[total_units].checksum       lake=171942461.0000 legacy=171942461.0000
PASS  col[total_units].mismatches     n=0
PASS  col[good_units].checksum        lake=168959976.0000 legacy=168959976.0000
PASS  col[good_units].mismatches      n=0
PASS  col[cases].checksum             lake=7450002.0000 legacy=7450002.0000
PASS  col[cases].mismatches           n=0
PASS  col[yield_pct].mismatches       n=0
```

## Divergences found and fixed

none — first reconciliation passed.

## Deviations from legacy: none
