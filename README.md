# mfg-analytics-etl

MES reporting estate for **Northwind Hygiene Products**.

Legacy stack: SQL Server stored procedures orchestrated by Azure Data
Factory (`legacy/`). Reporting tables live in the `rpt` schema; staging
in `mes`/`stg`; reference data in `dim`. Prod report outputs are exported
to `legacy_snapshots/` (see MANIFEST.md there).

Migration to the ADLS lakehouse is in progress — see `lakehouse/` for the
PySpark target and `tools/` for the seed / reconcile harness.

See `AGENTS.md` for how to work in the repo.
