# Legacy report extracts — MANIFEST

Exports of the `rpt.*` reporting tables from the production legacy server.
These are the golden outputs the ADLS migration is validated against.

| | |
|---|---|
| Source server | `NWH-MESRPT-SQL01` (prod, SQL Server 2022 CU14) |
| Database | `MES_Reporting` |
| Export date | 2025-11-17 |
| Exported by | ETL shared service (`svc_etl_batch`), nightly `PL_Master` run of 2025-11-17 with `AsOfUtc = 2025-11-17 00:00:00`, `WindowDays = 28` |
| Format | CSV, UTF-8, header row, NULL = empty field, datetimes `YYYY-MM-DD HH:MM:SS`, dates `YYYY-MM-DD` |

Export command shape (per table):

```bat
bcp "SELECT * FROM rpt.<table> ORDER BY <key>" queryout <table>.csv ^
    -S NWH-MESRPT-SQL01 -d MES_Reporting -c -t, -r\n -T
```

Files:

| file | rows | columns |
|---|---|---|
| rpt.line_downtime_daily.csv | 515 | plant_id char(5), line_id varchar(12), production_day date, shift_code varchar(4), reason_category varchar(30), planned_flag bit, event_count int, downtime_minutes int |
| rpt.daily_production.csv | 672 | plant_id char(5), line_id varchar(12), production_day date, sku_id varchar(10), total_units int, good_units int, cases int NULL, yield_pct decimal(9,2) NULL |
| rpt.oee_shift.csv | 1792 | plant_id char(5), line_id varchar(12), production_day date, shift_code varchar(4), planned_min int, unplanned_dt_min int, availability decimal(9,4) NULL, performance decimal(9,4) NULL, quality decimal(9,4) NULL, oee decimal(9,4) NULL |
| rpt.open_quality_holds.csv | 33 | lot_id varchar(30), line_id varchar(12), status varchar(20), status_ts_utc datetime2, open_minutes int |
| rpt.scrap_yield_weekly.csv | 96 | plant_id char(5), line_id varchar(12), iso_year int, iso_week int, good_units int, scrap_units int, scrap_pct decimal(9,2) NULL |
| rpt.material_variance.csv | 1057 | order_id varchar(14), material_code varchar(18) NULL, std_qty decimal(14,2) NULL, actual_qty decimal(14,2) NULL, variance_qty decimal(14,2) NULL, variance_label varchar(40) NULL |

Notes:

- `downtime_minutes` uses the legacy `DATEDIFF(MINUTE,...)` convention —
  it counts minute boundaries crossed, not elapsed seconds / 60.
- `open_quality_holds` reflects latest hold status per lot as of the
  `AsOfUtc` cutoff; open events are capped at the cutoff.
- Do not regenerate locally — these are prod extracts. Mismatches mean
  the conversion is wrong, not the extract.
