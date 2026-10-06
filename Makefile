PY      := .venv/bin/python
PIP     := .venv/bin/pip
JOBS_DIR := lakehouse/src/mfg_lake/jobs
export PYTHONPATH := lakehouse/src

.PHONY: setup seed legacy-up legacy-run legacy-down run reconcile test ci

setup:
	python3 -m venv .venv
	$(PIP) install -r requirements.txt -e lakehouse

seed:
	$(PY) tools/seed.py

legacy-up:
	docker compose up -d --wait mssql

legacy-run:
	$(PY) tools/legacy_run.py

legacy-down:
	docker compose down -v

run:
	$(PY) -m mfg_lake.jobs.$(JOB) --ns $(NS)

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
		$(PY) -m mfg_lake.jobs.$$name --ns ci || exit 1; \
		echo "== ci: reconcile $$report (ns=ci) =="; \
		$(PY) tools/reconcile.py --report $$report --ns ci || exit 1; \
	done
	@echo "CI OK"
