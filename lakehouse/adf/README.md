# lakehouse/adf

Converted ADF pipelines land here: Spark job / notebook activities that
call the PySpark jobs in `lakehouse/src/mfg_lake/jobs/` instead of the
legacy SqlServerStoredProcedure activities. Layout mirrors `legacy/adf/`.

| pipeline | replaces | job |
|---|---|---|
| `pipeline/PL_Line_Downtime.json` | `legacy/adf/pipeline/PL_Line_Downtime.json` (3 procs + Copy) | `mfg_lake.jobs.line_downtime_daily` |
| `pipeline/PL_Daily_Production.json` | `legacy/adf/pipeline/PL_Daily_Production.json` (2 procs + Copy) | `mfg_lake.jobs.daily_production` |
| `pipeline/PL_Scrap_Yield.json` | `legacy/adf/pipeline/PL_Scrap_Yield.json` (2 procs + Copy) | `mfg_lake.jobs.scrap_yield_weekly` |

Converted pipelines keep the legacy pipeline name and `AsOfUtc` parameter so
`PL_Master`'s `ExecutePipeline` activities (and the OEE dependency on
`EP_PL_Line_Downtime`, plus the `PL_OEE` / `PL_Scrap_Yield` dependencies on
`EP_PL_Daily_Production`) are unchanged. `linkedService/LS_Databricks_MfgLake.json`
holds placeholder workspace ids — fill in per environment.
