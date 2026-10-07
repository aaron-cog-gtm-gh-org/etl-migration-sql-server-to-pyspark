#!/usr/bin/env bash
# Validation run (recorded): reconcile the canonical seed, CI, fuzz.
#   tools/run_validation.sh [NS] [FUZZ_N] [REPORT]
set -euo pipefail
cd "$(dirname "$0")/.."
NS=${1:-kan6-validation}
N=${2:-25}
REPORT=${3:-daily_production}
case "$REPORT" in
  daily_production)    TICKET=KAN-6 ;;
  line_downtime_daily) TICKET=KAN-8 ;;
  *)                   TICKET=migration ;;
esac
if [ "$REPORT" = daily_production ]; then JUNIT=out/validation/pytest.xml
else JUNIT="out/validation/pytest_$REPORT.xml"; fi
step() { printf '\n\033[1;36m$ %s\033[0m\n' "$*"; "$@"; }
quiet() { grep -v -E 'WARN|setLogLevel|NativeCodeLoader|^Setting default log level' || true; }

echo "== $TICKET $REPORT validation  $(date -u '+%F %T') UTC  commit $(git rev-parse --short HEAD) =="
step pwd
step command -v python3
step sha256sum "legacy_snapshots/rpt.$REPORT.csv"
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
.venv/bin/python "tools/fuzz_$REPORT.py" --n "$N" 2>&1 | quiet | grep -v "^\[$REPORT\] wrote"
step sha256sum "legacy_snapshots/rpt.$REPORT.csv"
step git status --short legacy legacy_snapshots
echo "== done =="
