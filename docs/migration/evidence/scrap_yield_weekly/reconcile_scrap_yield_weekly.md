# Reconciliation: scrap_yield_weekly (ns=dev)

legacy: `rpt.scrap_yield_weekly` vs lake: `out/dev/curated/scrap_yield_weekly`

| control | detail | result |
|---|---|---|
| row_count | lake=96 legacy=96 | PASS |
| key_set | missing=0 extra=0 | PASS |
| col[plant_id].mismatches | n=0 | PASS |
| col[line_id].mismatches | n=0 | PASS |
| col[iso_year].checksum | lake=194400.0000 legacy=194400.0000 | PASS |
| col[iso_year].mismatches | n=0 | PASS |
| col[iso_week].checksum | lake=4272.0000 legacy=4272.0000 | PASS |
| col[iso_week].mismatches | n=0 | PASS |
| col[good_units].checksum | lake=168959976.0000 legacy=168959976.0000 | PASS |
| col[good_units].mismatches | n=0 | PASS |
| col[scrap_units].checksum | lake=138356.0000 legacy=138356.0000 | PASS |
| col[scrap_units].mismatches | n=0 | PASS |
| col[scrap_pct].checksum | lake=8.2000 legacy=8.2000 | PASS |
| col[scrap_pct].mismatches | n=0 | PASS |