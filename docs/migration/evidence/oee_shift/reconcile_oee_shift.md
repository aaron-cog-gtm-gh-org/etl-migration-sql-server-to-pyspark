# Reconciliation: oee_shift (ns=dev)

legacy: `rpt.oee_shift` vs lake: `out/dev/curated/oee_shift`

| control | detail | result |
|---|---|---|
| row_count | lake=1792 legacy=1792 | PASS |
| key_set | missing=0 extra=0 | PASS |
| col[plant_id].mismatches | n=0 | PASS |
| col[line_id].mismatches | n=0 | PASS |
| col[production_day].mismatches | n=0 | PASS |
| col[shift_code].mismatches | n=0 | PASS |
| col[planned_min].checksum | lake=968640.0000 legacy=968640.0000 | PASS |
| col[planned_min].mismatches | n=0 | PASS |
| col[unplanned_dt_min].checksum | lake=35719.0000 legacy=35719.0000 | PASS |
| col[unplanned_dt_min].mismatches | n=0 | PASS |
| col[availability].checksum | lake=1726.4051 legacy=1726.4051 | PASS |
| col[availability].mismatches | n=0 | PASS |
| col[performance].checksum | lake=1574.5355 legacy=1574.5355 | PASS |
| col[performance].mismatches | n=0 | PASS |
| col[quality].mismatches | n=0 | PASS |
| col[oee].mismatches | n=0 | PASS |