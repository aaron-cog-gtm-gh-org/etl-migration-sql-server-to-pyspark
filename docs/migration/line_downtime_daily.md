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
