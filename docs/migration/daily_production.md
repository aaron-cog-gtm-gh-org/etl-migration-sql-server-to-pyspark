# PL_Daily_Production -> `mfg_lake.jobs.daily_production` (KAN-6)

Legacy: `legacy/adf/pipeline/PL_Daily_Production.json` (+ the shift-calendar
step of `PL_Master`). Target: `lakehouse/src/mfg_lake/jobs/daily_production.py`,
pipeline `lakehouse/adf/pipeline/PL_Daily_Production.json`, curated report
`abfss://curated@kcmfglake.dfs.core.windows.net/manufacturing/daily_production`.

## Step mapping

| # | legacy step | PySpark equivalent | test(s) |
|---|---|---|---|
| 1 | `PL_Master.LKP_WindowStart` + `SP_RefreshShiftCalendar` -> `dim.usp_refresh_shift_calendar(@StartDate=MIN(dim.calendar), @Days=WindowDays)` | `build_shift_calendar()`: `dim.plant` x `dim.shift_pattern` x every `dim.calendar` date; local bounds = `calendar_date + local_start` / `+ end_next_day days + local_end`; UTC via `to_utc_timestamp(local, IANA(tz_name))` | `test_shift_calendar_*`, `test_fall_back_*`, `test_spring_forward_gap_shifts_forward` |
| 2 | `AT TIME ZONE p.tz_name` (Windows tz names) | `common.tz.windows_to_iana()` (CLDR windowsZones primary zone), applied driver-side to the 6-row `dim.plant` | `tests/test_tz.py` |
| 3 | `mes.usp_stg_production_counts`: `JOIN dim.line`, `JOIN dim.plant`, `JOIN dim.shift_calendar ON bucket_start_utc >= start_utc AND < end_utc` | `stage_production_local()`: same inner joins; non-equi range join on bucket **start** | `test_unknown_line_is_dropped`, `test_bucket_outside_shift_calendar_window_is_dropped`, `test_bucket_straddling_shift_boundary_kept_once` |
| 4 | `dim.ufn_production_day(bucket_start_utc AT TIME ZONE 'UTC' AT TIME ZONE tz)` = `CAST(DATEADD(HOUR,-6,local) AS DATE)` | `to_date(from_utc_timestamp(bucket_start_utc, tz) - INTERVAL 6 HOURS)` | `test_production_day_*`, `test_bucket_straddling_0600_attributed_by_start`, `test_mexico_has_no_dst` |
| 5 | `rpt.usp_rpt_daily_production`: `JOIN dim.sku`, `GROUP BY plant, line, day, sku, pack_size`, `SUM` | `build_report()`: inner join `dim.sku`, same grouping | `test_unknown_sku_is_dropped`, `test_duplicate_buckets_are_summed_like_legacy` |
| 6 | `cases = SUM(good_units) / k.pack_size` (INT / INT) | `good div pack_size` (truncating integral division) | `test_cases_*` |
| 7 | `ROUND(CAST(SUM(good) AS DECIMAL(18,4)) / NULLIF(SUM(total),0) * 100, 2)` into `DECIMAL(9,2)` | decimal division, `NULL` when total = 0, `round(.., 2)` (HALF_UP = T-SQL half away from zero), cast `decimal(9,2)` | `test_yield_null_when_total_zero`, `test_yield_round_half_away_from_zero` (98.125 -> 98.13) |
| 8 | `TRUNCATE` + `INSERT rpt.daily_production`, then `COPY_rpt_daily_production_to_curated` (Parquet sink) | `common.io.write_curated(df, "daily_production", ns)` (overwrite, parquet) | `make reconcile REPORT=daily_production` |
| 9 | `etl.usp_log_run` start/finish rows | stdout run line (input rows, dropped rows, staged rows, report rows) | - |
| 10 | `AsOfUtc` pipeline parameter (declared, never passed to the procs) | `--as-of-utc` accepted, not applied | `test_cli_accepts_ns_and_as_of_utc`; validation report (earlier cutoff -> identical output) |

## Deliberate deviations / decisions (need sign-off)

1. **NULL counts.** `mes.production_count` columns are `NOT NULL`, so legacy
   never sees them (the load would fail). The job drops rows with a NULL in
   any NOT NULL column and logs `dropped_not_null=<n>` instead of failing.
2. **INT overflow.** SQL Server `SUM(INT)` raises *Arithmetic overflow*;
   Spark sums as BIGINT. The job raises `ArithmeticError` if any group
   exceeds INT so the result is never silently wrapped.
3. **Shift calendar is computed in-job**, over the full `dim.calendar`
   window (= `PL_Master` `MIN(calendar_date)` + `WindowDays=28` when the
   calendar holds exactly 28 days, as it does). There is no
   `dim.shift_calendar` feed.
4. **Ambiguous / non-existent local times.** `to_utc_timestamp` (java.time)
   takes the earlier (DST) offset for a fall-back ambiguous time and shifts
   a spring-forward gap time forward by the gap. That is the documented
   `AT TIME ZONE` behaviour, but it was not verified against a live SQL
   Server. No shift bound in `dim.shift_pattern` (06/14/18/22:00) falls in
   either window, so this cannot affect the current report.
5. **"25h night shift".** The proc comment says the fall-back night shift
   is 25h. With today's patterns it is one hour longer than normal: 9h for
   3x8 plants (S3), 13h for 2x12 plants (N). Only a 24h pattern would be 25h
   (covered by `test_fall_back_24h_shift_is_25h`).
6. **`stg.production_local` is not persisted.** KAN-7/KAN-9 consume it.
   `stage_production_local()` is a public function they can import; persisting
   it as a curated/staging table is left to those tickets.
7. **`--as-of-utc` does not filter (KAN-6 AC4 conflict).** Per the
   requester's instruction and the legacy pipeline, the cutoff is not
   applied. An earlier cutoff therefore produces *identical* output and
   reconcile still passes. AC4's "earlier cutoff makes reconcile fail"
   can only be met by deviating from legacy (for example by filtering
   buckets on `bucket_start_utc < AsOfUtc`). That would also break parity
   with the extract, which contains production day 2025-11-16 buckets up to
   2025-11-17 ~12:00 UTC. The value-corruption and rerun-determinism parts
   of AC4 are met.

## Sign-off

| role | name | date |
|---|---|---|
| Data engineering | _pending_ | |
| Reporting owner | _pending_ | |
