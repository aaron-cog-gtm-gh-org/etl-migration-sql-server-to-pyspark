# lakehouse/adf

Converted ADF pipelines land here: Spark job / notebook activities that
call the PySpark jobs in `lakehouse/src/mfg_lake/jobs/` instead of the
legacy SqlServerStoredProcedure activities.

| pipeline | job | replaces |
|---|---|---|
| `pipeline/PL_Daily_Production.json` | `mfg_lake.jobs.daily_production` | `SP_StgProductionCounts`, `SP_RptDailyProduction`, `COPY_rpt_daily_production_to_curated` |

Contract kept for each converted pipeline: same pipeline name, same
`AsOfUtc` parameter (so `PL_Master`'s `ExecutePipeline` activities are
unchanged), same curated Parquet location
`abfss://curated@kcmfglake.dfs.core.windows.net/manufacturing/<report>`.

`linkedService/LS_Databricks_Mfg.json` is a placeholder (workspace ids are
not real); deploy-time values come from the ADF release, not this repo.
These JSON files are reviewed, not deployed, from here.
