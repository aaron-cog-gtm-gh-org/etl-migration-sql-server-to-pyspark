# PL_Line_Downtime -> `mfg_lake.jobs.line_downtime_daily` (KAN-8)

Legacy: `legacy/adf/pipeline/PL_Line_Downtime.json` (+ the shift-calendar
step of `PL_Master`). Target: `lakehouse/src/mfg_lake/jobs/line_downtime_daily.py`,
pipeline `lakehouse/adf/pipeline/PL_Line_Downtime.json`, curated report
`abfss://curated@kcmfglake.dfs.core.windows.net/manufacturing/line_downtime_daily`.
Snapshot: `legacy_snapshots/rpt.line_downtime_daily.csv` (515 rows).
Stacked on PR #5 (KAN-6, `daily_production`); reuses its `plant_zones`,
`build_shift_calendar` and `common.tz.windows_to_iana` (imported, not copied).

## Scope

### Inputs (legacy DDL)

| feed | columns used | notes |
|---|---|---|
| `mes.downtime_event` | `event_id INT PK`, `line_id VARCHAR(12)`, `start_utc DATETIME2(0)`, `end_utc DATETIME2(0) NULL`, `reason_code VARCHAR(10)`, `planned_flag BIT` | only `end_utc` is nullable (open event) |
| `dim.line` | `line_id`, `plant_id CHAR(5)` | inner join |
| `dim.plant` | `plant_id`, `tz_name` (Windows name) | inner join; `AT TIME ZONE tz_name` |
| `dim.shift_pattern` + `dim.calendar` | -> `dim.shift_calendar` | built by `PL_Master.SP_RefreshShiftCalendar`; reused from `daily_production.build_shift_calendar` |
| `dim.downtime_reason` | `reason_code`, `reason_category VARCHAR(30)` | inner join (report step only) |

### Output grain and types (`legacy/sql/schema/rpt.sql`, MANIFEST)

One row per `plant_id, line_id, production_day, shift_code, reason_category,
planned_flag` (= reconcile keys in `tools/reconcile_config.yaml`).

| column | legacy | lake |
|---|---|---|
| plant_id | CHAR(5) | string (trimmed) |
| line_id | VARCHAR(12) | string |
| production_day | DATE | date |
| shift_code | VARCHAR(4) | string |
| reason_category | VARCHAR(30) | string |
| planned_flag | BIT | boolean (bcp `1`/`0`) |
| event_count | INT | int |
| downtime_minutes | INT | int |

### AsOfUtc (verified in the pipeline JSON)

`PL_Line_Downtime` passes `@pipeline().parameters.AsOfUtc` to
`SP_StgDowntimeLocal` only. `mes.usp_stg_downtime_local` uses it twice:
`WHERE e.start_utc < @AsOfUtc` (events starting at/after the cutoff are
dropped) and `ISNULL(e.end_utc, @AsOfUtc)` (only NULL/open ends are capped;
closed events ending after the cutoff keep their real end). The job applies
exactly these two rules in `stage_downtime_local` and nowhere else. An
earlier cutoff therefore changes the report and must FAIL reconcile
(Jira AC4 is met).

### Proof of inputs

A throwaway pure-Python oracle (`csv` + `zoneinfo` + `windows_to_iana`, no
Spark) of the three procs over the canonical `data/raw/` reproduces the
snapshot byte-for-byte (CRLF bcp rendering): 450 staged events, 520
segments, 515 report rows. With an earlier cutoff it gives 369 / 426 / 421
rows and differs from the snapshot. No seed change is needed.

### Hazards and planned PySpark equivalent

| # | legacy behaviour | PySpark plan |
|---|---|---|
| H1 | `AT TIME ZONE p.tz_name` with Windows names; `GMT Standard Time` = Europe/London | `windows_to_iana` (raises on unknown names), `from_utc_timestamp` with session TZ = UTC |
| H2 | `start_utc < @AsOfUtc` (strict) and `ISNULL(end_utc, @AsOfUtc)` (NULL ends only) | same predicate / `coalesce(end_utc, as_of)` in `stage_downtime_local` |
| H3 | `mes.usp_split_downtime_by_shift` joins on **local** wall-clock bounds: `sc.start_local < e.end_local AND sc.end_local > e.start_local` (strict both sides) | non-equi join on local timestamps, same strict predicates; zero-length events on a shift boundary produce no segment |
| H4 | segment = `[max(start_local, shift_start_local), min(end_local, shift_end_local)]`; an event is split into every shift it overlaps (not attributed by start) | `greatest` / `least` |
| H5 | `production_day` = the shift calendar's `production_day` (shift start date), not `dim.ufn_production_day` of the event; a 03:00 local event belongs to the previous day's night shift | taken from `build_shift_calendar` output |
| H6 | `DATEDIFF(MINUTE, seg_start_local, seg_end_local)` counts minute boundaries crossed (10:00:59 -> 10:01:00 = 1), on **local** values | `floor(epoch(end)/60) - floor(epoch(start)/60)` on the local timestamps |
| H7 | fall-back: local DATEDIFF under-counts by 60 (a 9h fall-back night shift reports 480); spring-forward over-counts by 60 (7h -> 480) | preserved (DATEDIFF on local values) |
| H8 | an event entirely inside the repeated fall-back hour (01:50 CDT -> 01:10 CST) has `end_local < start_local`; the local join still matches the night shift and DATEDIFF is **negative** (-40) | preserved for parity, tested; flagged as a legacy bug |
| H9 | `COUNT(DISTINCT event_id)`, `SUM(DATEDIFF)`; `planned_flag` from the event (not `planned_default`) is part of the grain | `countDistinct`, `sum`; INT overflow raises |
| H10 | inner joins: unknown line (staging), unknown reason (report step), events outside the calendar window (no overlapping shift) are dropped | inner joins, dropped rows counted and logged |
| H11 | `mes.downtime_event` NOT NULL columns cannot be NULL in prod | rows with a NULL in a NOT NULL column are dropped and counted (deviation, see below) |
| H12 | CHAR(5) `plant_id` padding is ignored by SQL comparison | string keys trimmed on read |
| H13 | `@AsOfUtc DATETIME2(0)` | `--as-of-utc` parsed to whole-second UTC (`YYYY-MM-DD HH:MM:SS` or ISO-8601 with `T`/`Z`/offset) |
| H14 | `planned_flag BIT` exported by bcp as `1`/`0` | boolean in parquet; `tools/reconcile.py` normalises booleans to `1`/`0` generically |

### Ticket conflicts / open questions

- None blocking. KAN-8 AC4 (earlier cutoff must fail reconcile) is met
  because this pipeline really uses `AsOfUtc`.
- Shared helpers are imported from `daily_production` (PR #5) rather than
  moved into `mfg_lake/common/`, to keep parallel sibling branches from
  conflicting on a new common module; `daily_production.py` is unchanged.

## Public stages

Pure `DataFrame -> DataFrame` functions in `mfg_lake.jobs.line_downtime_daily`,
stable for downstream jobs (KAN-7 `PL_OEE` imports them instead of
re-deriving `stg.downtime_shift_seg`). Column tuples are exported as
constants. No stage reads the clock or applies a filter the legacy proc does
not.

| function | legacy equivalent | output columns (type) |
|---|---|---|
| `stage_downtime_local(downtime_event, line, plant, as_of_utc)` | `mes.usp_stg_downtime_local` -> `stg.downtime_local` | `STG_DOWNTIME_LOCAL_COLUMNS`: `event_id` int, `plant_id` string, `line_id` string, `reason_code` string, `planned_flag` boolean, `start_utc` timestamp, `end_utc` timestamp (NULL ends capped at `as_of_utc`), `start_local` timestamp, `end_local` timestamp |
| `split_downtime_by_shift(downtime_local, plant, shift_calendar)` | `mes.usp_split_downtime_by_shift` -> `stg.downtime_shift_seg` | `STG_DOWNTIME_SHIFT_SEG_COLUMNS`: `event_id` int, `plant_id` string, `line_id` string, `reason_code` string, `planned_flag` boolean, `production_day` date, `shift_code` string, `seg_start_local` timestamp, `seg_end_local` timestamp |
| `build_report(downtime_shift_seg, downtime_reason)` | `rpt.usp_rpt_line_downtime_daily` | `OUTPUT_COLUMNS` (types above) plus helper `_minutes_raw` bigint used by the INT-overflow guard; `transform` selects `OUTPUT_COLUMNS` only |
| `datediff_minute(start, end)` | `DATEDIFF(MINUTE, start, end)` | int Column expression |
| `parse_as_of_utc(value)` | `@AsOfUtc DATETIME2(0)` | naive UTC `datetime` |

Inputs:

- `downtime_event`: raw `mes.downtime_event` (`read_raw` schema or strings).
- `line`, `plant`, `downtime_reason`: raw `dim.*` feeds. String keys are trimmed.
- `as_of_utc`: a `datetime` or text accepted by `parse_as_of_utc`.
- `shift_calendar`: `daily_production.build_shift_calendar(dim.plant, dim.shift_pattern, dim.calendar)`.
- `downtime_local`: the output of `stage_downtime_local`.

`seg_*_local` are plant wall-clock timestamps (session TZ is UTC, so the
values print as local time). Downstream DATEDIFFs on them must use
`datediff_minute`, as `mes.usp_calc_planned_time` does.

## Step mapping

| # | legacy step | PySpark equivalent | test(s) |
|---|---|---|---|
| 1 | `PL_Master.SP_RefreshShiftCalendar` -> `dim.shift_calendar` | `daily_production.build_shift_calendar()` (PR #5), imported | `test_stages_compose_directly_and_are_pure`, DST tests |
| 2 | `SP_StgDowntimeLocal` -> `mes.usp_stg_downtime_local(@AsOfUtc)` -> `stg.downtime_local` | `stage_downtime_local()`: NOT NULL drops, inner joins `dim.line` / `dim.plant`, `start_utc < as_of`, `coalesce(end_utc, as_of)`, `from_utc_timestamp(.., windows_to_iana(tz_name))` | `test_event_starting_at_as_of_is_excluded`, `test_event_starting_one_second_before_as_of_is_included`, `test_open_event_is_capped_at_as_of`, `test_closed_event_past_as_of_keeps_its_real_end`, `test_unknown_line_is_dropped_in_staging`, `test_rows_violating_not_null_contract_are_dropped`, `test_char_padding_in_line_plant_id_still_joins`, `test_gmt_standard_time_is_europe_london`, `test_unmapped_tz_name_raises_keyerror` |
| 3 | `SP_SplitDowntimeByShift` -> `mes.usp_split_downtime_by_shift` -> `stg.downtime_shift_seg` | `split_downtime_by_shift()`: shift bounds UTC -> local, strict local overlap join, `greatest` / `least` clipping, `production_day` from the shift calendar | shift-boundary / midnight / 06:00 / zero-length tests, `test_event_inside_repeated_fall_back_hour_has_negative_minutes` |
| 4 | `SP_RptLineDowntimeDaily` -> `rpt.usp_rpt_line_downtime_daily`: `JOIN dim.downtime_reason`, `GROUP BY` 6 keys, `COUNT(DISTINCT event_id)`, `SUM(DATEDIFF(MINUTE, seg_start_local, seg_end_local))` | `build_report()` + `datediff_minute()` (minute-boundary count on local values); INT-overflow guard | `test_datediff_minute_*`, `test_fall_back_*`, `test_spring_forward_*`, `test_event_count_is_distinct_events`, `test_duplicate_event_id_counts_once_but_minutes_double`, `test_planned_flag_*`, `test_unknown_reason_survives_split_but_dropped_in_report` |
| 5 | `COPY_rpt_line_downtime_daily_to_curated` (Parquet sink) | `write_curated(df, "line_downtime_daily", ns)` (overwrite) | `make reconcile REPORT=line_downtime_daily` |
| 6 | ADF `PL_Line_Downtime` (AsOfUtc param, retry 2 / 60 s) | `lakehouse/adf/pipeline/PL_Line_Downtime.json`: one `DatabricksSparkPython` activity, `--ns` / `--as-of-utc @pipeline().parameters.AsOfUtc`, `LS_Databricks_Mfg`, same retry policy and sink | review |
| 7 | `tools/reconcile.py` BIT handling | `_norm`: bool / `np.bool_` -> `1` / `0`, generic | `tests/test_reconcile.py::test_bit_*`, `test_norm_*` |

## Deliberate deviations / decisions (need sign-off)

1. **NULLs in NOT NULL columns.** `mes.downtime_event` columns `event_id`,
   `line_id`, `start_utc`, `reason_code` and `planned_flag` are NOT NULL, so
   legacy never sees NULLs there. The job drops such rows and logs
   `dropped_not_null=<n>` instead of failing.
2. **INT overflow.** SQL Server `SUM(INT)` raises an arithmetic overflow, but
   Spark sums as BIGINT. The job raises `ArithmeticError` when a group's
   `downtime_minutes` exceeds the INT range.
3. **Duplicate `event_id`.** This is a PK violation and cannot happen in prod.
   The job behaves the way SQL would on such rows: `event_count` counts the
   event once, and its minutes are summed once per row. There is no dedupe.
4. **The shift calendar is computed in-job**, using
   `daily_production.build_shift_calendar` over the full `dim.calendar`
   window. There is no `dim.shift_calendar` feed.
5. **Ambiguous / non-existent local times** follow java.time / zoneinfo
   fold=0, the documented `AT TIME ZONE` behaviour. This was not verified
   against a live SQL Server.

Legacy bugs kept for parity (not fixed without approval):

- `DATEDIFF(MINUTE)` runs on local wall-clock values. Downtime across
  fall-back is under-counted by 60 minutes, and downtime across
  spring-forward is over-counted by 60.
- An event that lies entirely inside the repeated fall-back hour reports
  negative minutes.

## Ticket AC status (KAN-8)

| AC | status |
|---|---|
| Reconciles 100% vs `rpt.line_downtime_daily` snapshot (515 rows, keys, every column) | met: 13/13 controls, byte-identical CRLF CSV |
| DST / shift split / DATEDIFF semantics preserved | met: unit tests + fuzz oracle |
| Earlier `AsOfUtc` cutoff must fail reconcile (AC4) | met: AsOfUtc is applied as in `mes.usp_stg_downtime_local` |
| Deterministic, no wall clock | met: source-scan test, rerun determinism |
| ADF pipeline replaced, `PL_Master` unchanged | met (reviewed, not deployed) |
| Public stages for KAN-7 | met: see "Public stages" |

None are "not met".

## Coverage gaps

- `AT TIME ZONE` ambiguity handling was not checked against a live SQL
  Server. No `dim.shift_pattern` bound falls inside a DST transition, so the
  shift calendar is unaffected. Events inside the repeated hour are covered
  by tests and the fuzz oracle (zoneinfo).
- The ADF JSON was not deployed. Databricks runtime and ADLS writes were
  exercised only via the local lake root.
- The canonical snapshot covers only the fall-back transition. The
  spring-forward behaviour is covered by unit tests and the fuzz variants,
  which use a spring calendar window.

## Validation evidence

`tools/run_validation.sh kan8-validation 25 line_downtime_daily`, recorded,
in a clean worktree. Artifacts:

- `out/validation/line_downtime_daily_validation.html`
- `out/validation/fuzz_line_downtime_daily.json`
- `out/validation/pytest_line_downtime_daily.xml`
- `out/validation/fuzz_run_line_downtime_daily.mp4`
- `out/validation/fuzz_run_line_downtime_daily.cast`

## Sign-off

| role | name | date | decision |
|---|---|---|---|
| Migration engineer | Devin (KAN-8) | | implemented, validated |
| MES reporting owner | | | |
| Data platform | | | |
