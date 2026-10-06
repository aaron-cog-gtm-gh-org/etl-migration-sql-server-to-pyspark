# lakehouse/adf

Converted ADF pipelines land here: Spark job / notebook activities that
call the PySpark jobs in `lakehouse/src/mfg_lake/jobs/` instead of the
legacy SqlServerStoredProcedure activities. Layout mirrors `legacy/adf/`.

| pipeline | replaces | job |
|---|---|---|
| `pipeline/PL_Line_Downtime.json` | `legacy/adf/pipeline/PL_Line_Downtime.json` (3 procs + Copy) | `mfg_lake.jobs.line_downtime_daily` |

Converted pipelines keep the legacy pipeline name and `AsOfUtc` parameter so
`PL_Master`'s `ExecutePipeline` activities (and the OEE dependency on
`EP_PL_Line_Downtime`) are unchanged. `linkedService/LS_Databricks_MfgLake.json`
holds placeholder workspace ids — fill in per environment.
