# lakehouse/adf

Converted ADF pipelines land here: Spark job / notebook activities that
call the PySpark jobs in `lakehouse/src/mfg_lake/jobs/` instead of the
legacy SqlServerStoredProcedure activities.

| pipeline | job | replaces |
|---|---|---|
| `pipeline/PL_Daily_Production.json` | `mfg_lake.jobs.daily_production` | `SP_StgProductionCounts`, `SP_RptDailyProduction`, `COPY_rpt_daily_production_to_curated` |
| `pipeline/PL_Line_Downtime.json` | `mfg_lake.jobs.line_downtime_daily` | `SP_StgDowntimeLocal`, `SP_SplitDowntimeByShift`, `SP_RptLineDowntimeDaily`, `COPY_rpt_line_downtime_daily_to_curated` |
| `pipeline/PL_OEE.json` | `mfg_lake.jobs.oee_shift` | `SP_CalcPlannedTime`, `SP_RptOeeShift`, `COPY_rpt_oee_shift_to_curated` |

Contract kept for each converted pipeline: same pipeline name, same
`AsOfUtc` parameter (so `PL_Master`'s `ExecutePipeline` activities are
unchanged), same curated Parquet location
`abfss://curated@kcmfglake.dfs.core.windows.net/manufacturing/<report>`.

`linkedService/LS_Databricks_Mfg.json` is a placeholder (workspace ids are
not real); deploy-time values come from the ADF release, not this repo.
These JSON files are reviewed, not deployed, from here.
