# Reconciliation: line_downtime_daily (ns=ci)

legacy: `rpt.line_downtime_daily` vs lake: `out/ci/curated/line_downtime_daily`

| control | detail | result |
|---|---|---|
| row_count | lake=515 legacy=515 | PASS |
| key_set | missing=0 extra=0 | PASS |
| col[plant_id].mismatches | n=0 | PASS |
| col[line_id].mismatches | n=0 | PASS |
| col[production_day].mismatches | n=0 | PASS |
| col[shift_code].mismatches | n=0 | PASS |
| col[reason_category].mismatches | n=0 | PASS |
| col[planned_flag].checksum | lake=2.0000 legacy=2.0000 | PASS |
| col[planned_flag].mismatches | n=0 | PASS |
| col[event_count].checksum | lake=520.0000 legacy=520.0000 | PASS |
| col[event_count].mismatches | n=0 | PASS |
| col[downtime_minutes].checksum | lake=35869.0000 legacy=35869.0000 | PASS |
| col[downtime_minutes].mismatches | n=0 | PASS |