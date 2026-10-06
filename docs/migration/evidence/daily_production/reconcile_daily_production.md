# Reconciliation: daily_production (ns=dev)

legacy: `rpt.daily_production` vs lake: `out/dev/curated/daily_production`

| control | detail | result |
|---|---|---|
| row_count | lake=672 legacy=672 | PASS |
| key_set | missing=0 extra=0 | PASS |
| col[plant_id].mismatches | n=0 | PASS |
| col[line_id].mismatches | n=0 | PASS |
| col[production_day].mismatches | n=0 | PASS |
| col[sku_id].mismatches | n=0 | PASS |
| col[total_units].checksum | lake=171942461.0000 legacy=171942461.0000 | PASS |
| col[total_units].mismatches | n=0 | PASS |
| col[good_units].checksum | lake=168959976.0000 legacy=168959976.0000 | PASS |
| col[good_units].mismatches | n=0 | PASS |
| col[cases].checksum | lake=7450002.0000 legacy=7450002.0000 | PASS |
| col[cases].mismatches | n=0 | PASS |
| col[yield_pct].mismatches | n=0 | PASS |