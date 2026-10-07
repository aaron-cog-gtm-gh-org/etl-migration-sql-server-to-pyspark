#!/usr/bin/env bash
# KAN-6 validation run (recorded): reconcile the canonical seed, CI, fuzz.
#   tools/run_validation.sh [NS] [FUZZ_N]
set -euo pipefail
cd "$(dirname "$0")/.."
NS=${1:-kan6-validation}
N=${2:-25}
step() { printf '\n\033[1;36m$ %s\033[0m\n' "$*"; "$@"; }
quiet() { grep -v -E 'WARN|setLogLevel|NativeCodeLoader|^Setting default log level' || true; }

echo "== KAN-6 daily_production validation  $(date -u '+%F %T') UTC  commit $(git rev-parse --short HEAD) =="
step pwd
step command -v python3
step sha256sum legacy_snapshots/rpt.daily_production.csv
step make seed
step git status --short data/raw
printf '\n\033[1;36m$ make run JOB=daily_production NS=%s\033[0m\n' "$NS"
make run JOB=daily_production NS="$NS" 2>&1 | quiet
step make reconcile REPORT=daily_production NS="$NS"
printf '\n\033[1;36m$ pytest tests -q --junitxml out/validation/pytest.xml\033[0m\n'
mkdir -p out/validation
.venv/bin/python -m pytest tests -q -p no:cacheprovider --junitxml out/validation/pytest.xml 2>&1 | quiet
printf '\n\033[1;36m$ make ci\033[0m\n'
make ci 2>&1 | quiet
printf '\n\033[1;36m$ python tools/fuzz_daily_production.py --n %s\033[0m\n' "$N"
.venv/bin/python tools/fuzz_daily_production.py --n "$N" 2>&1 | quiet | grep -v '^\[daily_production\] wrote'
step sha256sum legacy_snapshots/rpt.daily_production.csv
step git status --short legacy legacy_snapshots
echo "== done =="
