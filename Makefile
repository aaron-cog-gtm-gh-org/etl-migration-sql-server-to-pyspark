PY      := .venv/bin/python
PIP     := .venv/bin/pip
JOBS_DIR := lakehouse/src/mfg_lake/jobs
# report cutoff passed to every job's --as-of-utc (matches the legacy
# PL_Master AsOfUtc parameter and the snapshot export in MANIFEST.md)
AS_OF_UTC ?= 2025-11-17 00:00:00
export PYTHONPATH := lakehouse/src

.PHONY: setup seed run reconcile test ci

setup:
	python3 -m venv .venv
	$(PIP) install -r requirements.txt -e lakehouse

seed:
	$(PY) tools/seed.py

run:
	$(PY) -m mfg_lake.jobs.$(JOB) --ns $(NS) --as-of-utc "$(AS_OF_UTC)"

reconcile:
	$(PY) tools/reconcile.py --report $(REPORT) --ns $(NS)

test:
	$(PY) -m pytest tests -q

ci: test
	@for f in $(JOBS_DIR)/*.py; do \
		name=$$(basename $$f .py); \
		[ "$$name" = "__init__" ] && continue; \
		report=$$($(PY) tools/reconcile.py --list-jobs 2>/dev/null | grep -x $$name || true); \
		if [ -z "$$report" ]; then report=$$name; fi; \
		echo "== ci: run $$name (ns=ci) =="; \
		$(PY) -m mfg_lake.jobs.$$name --ns ci --as-of-utc "$(AS_OF_UTC)" || exit 1; \
		echo "== ci: reconcile $$report (ns=ci) =="; \
		$(PY) tools/reconcile.py --report $$report --ns ci || exit 1; \
	done
	@echo "CI OK"
