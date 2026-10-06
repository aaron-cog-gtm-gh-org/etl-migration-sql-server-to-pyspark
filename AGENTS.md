# Working in this repo

Environment mechanics only. Everything below runs locally.

## Setup

```bash
make setup            # creates .venv, installs python deps (pyspark, pandas, ...)
```

Requires: python 3.10+, Java 17 (for PySpark), Docker (for legacy SQL Server).

## Seed data

```bash
make seed             # regenerates data/raw/*.csv deterministically
```

## Legacy SQL Server

```bash
make legacy-up        # docker compose up, waits for healthy
make legacy-run       # create schema, bulk load data/raw, run PL_Master's proc
                      # sequence, export rpt.* tables to legacy_snapshots/
make legacy-down      # stop the container
```

`tools/legacy_run.py` connects to the SQL Server docker-compose service,
creates schemas/tables, bulk loads the raw CSVs, executes the same proc
sequence the ADF `PL_Master` pipeline runs, then exports every `rpt.*`
table to `legacy_snapshots/<table>.csv`.

## Lakehouse jobs

```bash
make run JOB=<module> NS=<ns>          # python -m mfg_lake.jobs.<module>
make reconcile REPORT=<report> NS=<ns> # compare out/<ns>/curated/<report>
                                       # vs legacy_snapshots/<table>.csv
```

Reports are registered in `tools/reconcile_config.yaml`. `make ci` runs the
test suite plus run+reconcile for every job present in
`lakehouse/src/mfg_lake/jobs/`.

Local lake root defaults to `./out/<NS>`; `tools/lakehouse/paths` resolves
the `abfss://curated@nwhmfglake.dfs.core.windows.net/manufacturing/<report>`
contract path to a local directory via `$LAKE_ROOT`.

## CI

`.github/workflows/ci.yml` runs `make ci` on python 3.11 + Java 17. No
docker in CI — reconciles against committed `legacy_snapshots/`.
