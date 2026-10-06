# Source-to-target mapping: `rpt.line_downtime_daily` → curated `line_downtime_daily`

| | Legacy | Target |
|---|---|---|
| Orchestration | ADF `PL_Line_Downtime` (`legacy/adf/pipeline/`): `SP_StgDowntimeLocal` → `SP_SplitDowntimeByShift` → `SP_RptLineDowntimeDaily` → `COPY_rpt_line_downtime_daily_to_curated` | ADF `PL_Line_Downtime` (`lakehouse/adf/pipeline/`): single `SPK_LineDowntimeDaily` (DatabricksSparkPython) |
| Code | `mes.usp_stg_downtime_local`, `mes.usp_split_downtime_by_shift`, `rpt.usp_rpt_line_downtime_daily` (+ `dim.usp_refresh_shift_calendar` from `PL_Master`) | `lakehouse/src/mfg_lake/jobs/line_downtime_daily.py` |
| Parameter | `AsOfUtc` (string → `DATETIME2(0)`) | `--as-of-utc` (same pipeline parameter), injected with `F.lit(...).cast("timestamp")`; never `current_timestamp()` |
| Output | `rpt.line_downtime_daily` → `abfss://curated@nwhmfglake.dfs.core.windows.net/manufacturing/line_downtime_daily` | same path (`mfg_lake.common.paths.abfss_uri`); parquet, **full overwrite** (≡ `TRUNCATE`+`INSERT`+Copy) |

## Grain

One row per `(plant_id, line_id, production_day, shift_code, reason_category, planned_flag)`.
Reconcile keys in `tools/reconcile_config.yaml` match.

## Column mapping

| target column | type (legacy → parquet) | source / derivation |
|---|---|---|
| `plant_id` | `CHAR(5)` → `string` | `dim.line.plant_id` via `mes.downtime_event.line_id`; **trimmed** (CHAR padding removed) |
| `line_id` | `VARCHAR(12)` → `string` | `mes.downtime_event.line_id` |
| `production_day` | `DATE` → `date` | `dim.shift_calendar.production_day` of the overlapping shift |
| `shift_code` | `VARCHAR(4)` → `string` | `dim.shift_calendar.shift_code` (from `dim.shift_pattern`) |
| `reason_category` | `VARCHAR(30)` → `string` | `dim.downtime_reason.reason_category`, **inner** join on `reason_code` |
| `planned_flag` | `BIT` → `boolean` | `mes.downtime_event.planned_flag` (event flag, not `planned_default`) |
| `event_count` | `INT` → `int` | `COUNT(DISTINCT event_id)` per grain |
| `downtime_minutes` | `INT` → `int` | `SUM(DATEDIFF(MINUTE, seg_start_local, seg_end_local))` per grain |

## Stage semantics

### 1. `mes.usp_stg_downtime_local` → `stage_downtime_local()`
- `mes.downtime_event ⋈ dim.line ⋈ dim.plant` (inner).
- Filter `start_utc < AsOfUtc`.
- `end_utc = coalesce(end_utc, AsOfUtc)` — only **open** (NULL-end) events are capped. Closed events that end after the cutoff keep their real end, exactly like `ISNULL`.
- `start_local/end_local = from_utc_timestamp(x, iana)` ≡ `x AT TIME ZONE 'UTC' AT TIME ZONE p.tz_name`.
- All timestamps `date_trunc('second', …)` ≡ `CAST(… AS DATETIME2(0))`.

### Shift calendar (`dim.usp_refresh_shift_calendar`) → `build_shift_calendar()`
`dim.shift_calendar` is not a raw feed, so it is rebuilt from `dim.plant × dim.shift_pattern × dim.calendar`: local bound = `calendar_date + local_start` (`+ end_next_day` days for `local_end`), → UTC with `to_utc_timestamp(local, iana)`. java.time resolves ambiguous fall-back wall times to the pre-transition offset and shifts spring-forward gap times forward, as SQL Server `AT TIME ZONE` does. E.g. PLT01 S3 on 2025-11-01 is 22:00 CDT → 06:00 CST = 03:00Z → 12:00Z (9 h).

### 2. `mes.usp_split_downtime_by_shift` → `split_by_shift()`
- Legacy joins on **local** bounds (`sc_start_local < end_local AND sc_end_local > start_local`), converting each shift-calendar row per join.
- Target joins on **UTC** (`sc.start_utc < e.end_utc AND sc.end_utc > e.start_utc`), clips `seg_start = greatest(e.start_utc, sc.start_utc)`, `seg_end = least(e.end_utc, sc.end_utc)`, then converts each segment bound to local once.
- Equivalence: UTC→local is order-preserving except inside a DST fold; shift boundaries (06/14/18/22:00 local) never fall in a fold, so the predicates and the clipped bounds are identical. Verified on the full seed feed by `tests/test_line_downtime_daily.py::test_utc_join_equivalent_to_legacy_local_join_on_seed`, which compares against `split_by_shift_local_legacy()` (a literal port of the proc): 0 differing segments both ways. If a plant ever gets a shift boundary inside 01:00–02:00 on a fall-back day, re-run that check.

### 3. `rpt.usp_rpt_line_downtime_daily` → `rollup_daily()`
- Inner join `dim.downtime_reason` on `reason_code`.
- `DATEDIFF(MINUTE, a, b)` counts **minute boundaries crossed**: `floor(epoch(b)/60) - floor(epoch(a)/60)` (`datediff_minute()`), not elapsed seconds / 60. `10:00:59 → 10:01:00` = 1; `10:00:00 → 10:00:59` = 0.
- Computed on **local wall-clock** segment bounds, as the legacy does. Across a fall-back the result is elapsed − 60: PLT01-L1 event 00:30 CDT → 02:30 CST on 2025-11-02 is 180 elapsed minutes and reports **120** (matches the snapshot).
- `epoch()` uses `unix_timestamp`, which reads naive wall-clock correctly only because `get_spark` pins `spark.sql.session.timeZone=UTC` (`tests/test_spark_tz.py`).

## Windows → IANA time zones

`dim.plant.tz_name` holds Windows names. `mfg_lake.common.tz.WINDOWS_TO_IANA` (CLDR `windowsZones`, territory 001) maps them; an unmapped name **fails the job** (no silent UTC fallback).

| plant | Windows | IANA |
|---|---|---|
| PLT01 | Central Standard Time | America/Chicago |
| PLT02 | Eastern Standard Time | America/New_York |
| PLT03 | Pacific Standard Time | America/Los_Angeles |
| PLT04 | Central Standard Time (Mexico) | America/Mexico_City (no DST since 2022) |
| PLT05 | E. South America Standard Time | America/Sao_Paulo |
| PLT06 | GMT Standard Time | Europe/London |

## AsOfUtc parsing

`parse_as_of_utc()` takes the Makefile form `2025-11-17 00:00:00` and the ADF `@trigger().scheduledTime` form `2025-11-17T06:30:00.0000000Z` (and `±hh:mm` offsets, converted to UTC). Fractional seconds round half-up, matching SQL Server's string → `DATETIME2(0)` conversion.

## Dropped / filtered row logging

The job logs (logger `mfg_lake.jobs.line_downtime_daily`), replacing `etl.usp_log_run`:

| stage | logged |
|---|---|
| 1 | events kept vs source (dropped by `start_utc >= AsOfUtc` or no `dim.line`/`dim.plant` match); open events capped at AsOfUtc |
| 2 | segment count, shift-calendar rows, events that overlap no shift (outside the calendar window) |
| 3 | segments dropped by the `dim.downtime_reason` inner join, at WARNING level with the distinct unmatched `reason_code`s (INFO "dropped 0" when none) |
| write | rows written + destination |

Seed run at `AsOfUtc = 2025-11-17 00:00:00`: 450/455 events, 1 open event capped, 520 segments, 0 events overlapping no shift, 0 dropped on reason join, 515 rows. See `evidence/line_downtime_daily/`.

## Reconciliation

`make ci` (or `make run JOB=line_downtime_daily NS=<ns>` + `make reconcile REPORT=line_downtime_daily NS=<ns>`) against `legacy_snapshots/rpt.line_downtime_daily.csv`: 13/13 controls PASS (row count 515, key set 0 missing / 0 extra, 0 value mismatches on every column, `event_count`/`downtime_minutes` checksums equal). No stage (`stg.*`) snapshots are in `legacy_snapshots/`, so stages are checked by unit tests and the UTC/local join equivalence test.

`tools/reconcile.py` now normalises boolean cells to `0`/`1` (how bcp exports `BIT`), so a `boolean` `planned_flag` reconciles against the snapshot, including as a key.
