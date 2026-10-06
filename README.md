# mfg-analytics-etl

MES reporting estate for **Northwind Hygiene Products**.

Legacy stack: SQL Server 2022 stored procedures orchestrated by Azure Data
Factory (`legacy/`). Reporting tables live in the `rpt` schema; staging in
`mes`/`stg`; reference data in `dim`.

Migration to the ADLS lakehouse is in progress — see `lakehouse/` for the
PySpark target and `tools/` for the seed / legacy-run / reconcile harness.

See `AGENTS.md` for how to stand the environment up.
