#!/usr/bin/env bash
# Validation run (recorded): reconcile the canonical seed, CI, fuzz.
#   tools/run_validation.sh [NS] [FUZZ_N] [REPORT]
# REPORT defaults to daily_production; the fuzz tool is tools/fuzz_${REPORT}.py
# and the snapshot is legacy_snapshots/rpt.${REPORT}.csv.
set -euo pipefail
cd "$(dirname "$0")/.."
REPORT=${3:-daily_production}
NS=${1:-kan6-validation}
N=${2:-25}
JUNIT=out/validation/pytest.xml
if [ "$REPORT" != "daily_production" ]; then
  NS=${1:-${REPORT}-validation}
  JUNIT=out/validation/pytest_${REPORT}.xml
fi
step() { printf '\n\033[1;36m$ %s\033[0m\n' "$*"; "$@"; }
quiet() { grep -v -E 'WARN|setLogLevel|NativeCodeLoader|^Setting default log level' || true; }

if [ "$REPORT" = "daily_production" ]; then
  echo "== KAN-6 daily_production validation  $(date -u '+%F %T') UTC  commit $(git rev-parse --short HEAD) =="
else
  echo "== ${REPORT} validation  $(date -u '+%F %T') UTC  commit $(git rev-parse --short HEAD) =="
fi
step pwd
step command -v python3
step sha256sum "legacy_snapshots/rpt.${REPORT}.csv"
step make seed
step git status --short data/raw
printf '\n\033[1;36m$ make run JOB=%s NS=%s\033[0m\n' "$REPORT" "$NS"
make run JOB="$REPORT" NS="$NS" 2>&1 | quiet
step make reconcile REPORT="$REPORT" NS="$NS"
printf '\n\033[1;36m$ pytest tests -q --junitxml %s\033[0m\n' "$JUNIT"
mkdir -p out/validation
.venv/bin/python -m pytest tests -q -p no:cacheprovider --junitxml "$JUNIT" 2>&1 | quiet
printf '\n\033[1;36m$ make ci\033[0m\n'
make ci 2>&1 | quiet
printf '\n\033[1;36m$ python tools/fuzz_%s.py --n %s\033[0m\n' "$REPORT" "$N"
.venv/bin/python "tools/fuzz_${REPORT}.py" --n "$N" 2>&1 | quiet | grep -v "^\[${REPORT}\] wrote"
step sha256sum "legacy_snapshots/rpt.${REPORT}.csv"
step git status --short legacy legacy_snapshots
echo "== done =="
