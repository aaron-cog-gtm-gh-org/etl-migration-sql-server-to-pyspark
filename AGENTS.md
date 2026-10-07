# Working in this repo

Environment mechanics only. Everything below runs locally.

## Setup

```bash
make setup            # creates .venv, installs python deps (pyspark, pandas, ...)
```

Requires: python 3.10+, Java 17 (for PySpark).

## Seed data

```bash
make seed             # regenerates data/raw/*.csv deterministically
```

`data/raw/` emulates the raw mes.*/dim.* feeds the estate consumes.

## Legacy estate (read-only reference)

`legacy/sql/` (schemas, functions, procs) and `legacy/adf/` (pipeline,
dataset, linkedService, trigger JSON) document what the legacy system
does today. Treat them as reference — there is no local SQL Server to
run them against.

Expected report outputs are prod extracts in `legacy_snapshots/` (see
`legacy_snapshots/MANIFEST.md`). Do not regenerate them.

## Lakehouse jobs

```bash
make run JOB=<module> NS=<ns>          # python -m mfg_lake.jobs.<module> \
                                       #   --ns <ns> --as-of-utc <cutoff>
make reconcile REPORT=<report> NS=<ns> # compare out/<ns>/curated/<report>
                                       # vs legacy_snapshots/<table>.csv
```

Job convention: every job takes `--ns` and `--as-of-utc`. `--as-of-utc` is
the report cutoff the legacy `AsOfUtc` proc parameter carried; `make run`
and `make ci` pass it from `AS_OF_UTC` in the Makefile (match
`legacy_snapshots/MANIFEST.md`).

Reports are registered in `tools/reconcile_config.yaml`. `make ci` runs the
test suite plus run+reconcile for every job present in
`lakehouse/src/mfg_lake/jobs/`.

Local lake root defaults to `./out/<NS>`; `lakehouse/src/mfg_lake/common/paths.py` resolves
the `abfss://curated@kcmfglake.dfs.core.windows.net/manufacturing/<report>`
contract path to a local directory via `$LAKE_ROOT`.

## CI

`.github/workflows/ci.yml` runs `make ci` on python 3.11 + Java 17 —
reconciles against the committed `legacy_snapshots/` extracts.
