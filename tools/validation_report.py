#!/usr/bin/env python3
"""Render out/validation/<report>_validation.html from the fuzz JSON
log, the reconcile contract and the pytest junit XML.

    python tools/validation_report.py [--report <report>] [--embed-video]

--report defaults to daily_production and reproduces the original KAN-6
page. For other reports the log's `report_meta` block supplies the ticket,
pipeline, table, parity notes and earlier-cutoff expectation; without it
the KAN-6 text is used as fallback.
"""
import argparse
import base64
import html
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
VAL = ROOT / "out" / "validation"
E = html.escape


def ddl_columns(table="rpt.daily_production"):
    sql = (ROOT / "legacy" / "sql" / "schema" / "rpt.sql").read_text()
    body = re.search(rf"CREATE TABLE {re.escape(table)} \((.*?)\n\);", sql, re.S).group(1)
    cols = []
    for line in body.strip().splitlines():
        m = re.match(r"\s*(\w+)\s+([\w(), ]+?)\s+(NOT NULL|NULL)", line)
        if m:
            cols.append((m.group(1), m.group(2).strip(), m.group(3)))
    return cols


LAKE_TYPE = {"CHAR(5)": "string", "VARCHAR(12)": "string", "VARCHAR(10)": "string",
             "DATE": "date32[day]", "INT": "int32", "DECIMAL(9,2)": "decimal128(9, 2)"}


def badge(ok, yes="PASS", no="FAIL"):
    return f'<span class="b {"ok" if ok else "ko"}">{yes if ok else no}</span>'


def junit(path):
    if not path.exists():
        return None
    root = ET.parse(path).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    cases = []
    for c in suite.iter("testcase"):
        status = "fail" if c.find("failure") is not None or c.find("error") is not None else (
            "skip" if c.find("skipped") is not None else "pass")
        cases.append((c.get("classname"), c.get("name"), status, c.get("time")))
    return {"tests": int(suite.get("tests")), "failures": int(suite.get("failures")),
            "errors": int(suite.get("errors")), "time": suite.get("time"), "cases": cases}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="daily_production")
    a0, _ = ap.parse_known_args()
    rep = a0.report
    suffix = "" if rep == "daily_production" else f"_{rep}"
    ap.add_argument("--log", default=str(VAL / f"fuzz_{rep}.json"))
    ap.add_argument("--out", default=str(VAL / f"{rep}_validation.html"))
    ap.add_argument("--video", default=f"fuzz_run{suffix}.mp4")
    ap.add_argument("--cast", default=f"fuzz_run{suffix}.cast")
    ap.add_argument("--embed-video", action="store_true")
    a = ap.parse_args()
    r = json.loads(Path(a.log).read_text())
    meta = r.get("report_meta") or {}
    ticket = meta.get("ticket", "KAN-6")
    pipeline = meta.get("pipeline", "PL_Daily_Production")
    table = meta.get("table", "rpt.daily_production")
    junit_xml = VAL / ("pytest.xml" if rep == "daily_production" else f"pytest_{rep}.xml")
    cfg = yaml.safe_load((ROOT / "tools" / "reconcile_config.yaml").read_text())
    env, c, s = r["environment"], r["canonical"], r["summary"]
    bc = c["byte_compare"]
    tests = junit(junit_xml)
    checks = []  # (section, check, ok, detail)

    # ---- schema contract
    schema_inv = c["schema"]["schema_contract"]
    snap_header = (ROOT / "legacy_snapshots" / f"{table}.csv").read_text().splitlines()[0]
    rows = []
    for name, typ, null in ddl_columns(table):
        exp = LAKE_TYPE.get(typ.replace(" ", ""), "?")
        in_snap = name in snap_header.split(",")
        rows.append(f"<tr><td>{E(name)}</td><td>{E(typ)} {null}</td><td>{E(exp)}</td>"
                    f"<td>{badge(in_snap, 'yes', 'no')}</td></tr>")
    checks.append(("Schema", f"parquet columns/types == {table} DDL",
                   schema_inv["pass"], schema_inv["detail"]))

    # ---- reconcile
    ctrl = []
    for line in c["reconcile"]["output"].splitlines():
        m = re.match(r"(PASS|FAIL)\s+(\S+)\s+(.*)", line)
        if m:
            ctrl.append(m.groups())
            checks.append(("Reconcile", m.group(2), m.group(1) == "PASS", m.group(3)))
    checks.append(("Reconcile", "byte-compare bcp-format CSV vs snapshot", bc["pass"],
                   f"sha256 {bc['lake_csv_sha256'][:16]}… vs {bc['snapshot_sha256'][:16]}…"))
    checks.append(("Adversarial", "rerun into same namespace is identical",
                   c["rerun_deterministic"]["pass"], "sorted dataframe + dtypes + CSV bytes"))
    checks.append(("Adversarial", "single corrupted value fails reconcile",
                   c["corrupted_value"]["pass"],
                   f"{c['corrupted_value']['mutation']} -> exit {c['corrupted_value']['reconcile_exit_code']}"))
    ec = c["earlier_cutoff"]
    ec_note = meta.get("earlier_cutoff_note",
                       "KAN-6 AC4 expects a FAIL here (see deviations)")
    checks.append(("Adversarial", f"earlier cutoff {ec['as_of_utc']} (as-of not applied)",
                   ec["pass"], f"identical={ec['identical_to_canonical']}, reconcile exit "
                   f"{ec['reconcile_exit_code']}; {ec_note}"))
    for name, inv in c["schema"].items():
        if not name.startswith("_") and name != "schema_contract":
            checks.append(("Canonical invariants", name, inv["pass"], inv["detail"]))
    for name, m in r.get("checker_self_test", {}).items():
        checks.append(("Checker self-test", f"mutant {name} is caught", m["caught"],
                       ", ".join(m["tripped"]) or "not caught"))
    if tests:
        for cls, name, st, t in tests["cases"]:
            checks.append(("Unit tests", f"{cls.split('.')[-1]}::{name}", st == "pass", f"{t}s"))

    # ---- fuzz variants
    vrows = []
    for v in r["variants"]:
        inp = v.get("inputs", {})
        feats = ", ".join(f"{k}={n}" for k, n in sorted(v["features"].items()))
        viol = "".join(f"<div><b>{E(k)}</b>: {E(v['invariants'][k]['detail'])}</div>"
                       for k in v["violations"]) or "—"
        vrows.append(
            f"<tr class='{'' if v['pass'] else 'bad'}'><td>{v['variant']:03d}</td>"
            f"<td>{v['rng_seed']}</td><td>{E(v['calendar_window'])}</td><td>{E(v['as_of_utc'])}</td>"
            f"<td>{inp.get('rows', '?')}</td><td>{inp.get('null_rows', '?')}</td>"
            f"<td>{inp.get('output_rows', '?')}</td><td class='f'>{E(feats)}</td>"
            f"<td>{badge(v['pass'])}</td><td>{viol}</td></tr>")
        checks.append(("Fuzz", f"variant {v['variant']:03d} ({v['calendar_window']})", v["pass"],
                       ", ".join(v["violations"]) or f"{len(v['invariants'])} invariants hold"))
    inv_names = sorted({k for v in r["variants"] for k in v["invariants"]})

    if a.embed_video and (VAL / a.video).exists():
        src = "data:video/mp4;base64," + base64.b64encode((VAL / a.video).read_bytes()).decode()
    else:
        src = a.video
    n_ok = sum(1 for *_, ok, _ in checks if ok)
    all_rows = "".join(
        f"<tr class='{'' if ok else 'bad'}'><td>{i}</td><td>{E(sec)}</td><td>{E(name)}</td>"
        f"<td>{badge(ok)}</td><td>{E(str(det))}</td></tr>"
        for i, (sec, name, ok, det) in enumerate(checks, 1))
    env_rows = "".join(f"<tr><td>{E(k)}</td><td>{E(str(v))}</td></tr>" for k, v in env.items())
    ctrl_rows = "".join(f"<tr><td>{E(n)}</td><td>{E(d)}</td><td>{badge(p == 'PASS')}</td></tr>"
                        for p, n, d in ctrl)
    overall = s["overall_pass"]
    if meta.get("fuzz_intro"):
        fuzz_intro = meta["fuzz_intro"]
    elif meta:
        fuzz_intro = (f"{s['variants_total']} variants (fuzz seed {env['fuzz_seed']}). "
                      "Each variant ran the job in a throwaway namespace and was checked for:\n"
                      + ", ".join(f"<code>{E(n)}</code>" for n in inv_names)
                      + ". The oracle invariants compare every output row (and the scrap "
                      "allocation) against an independent pure-Python port of the legacy procs.")
    else:
        fuzz_intro = (f"{s['variants_total']} variants of <code>mes.production_count</code> "
                      f"(fuzz seed {env['fuzz_seed']}). Each variant ran the job in a "
                      "throwaway namespace and was checked for:\n"
                      + ", ".join(f"<code>{E(n)}</code>" for n in inv_names)
                      + ". <code>oracle_values</code> / <code>production_day_local_rule</code> "
                      "compare every output row against an independent pure-Python "
                      "implementation of the legacy procs (zoneinfo, Decimal "
                      "half-away-from-zero).")
    parity_lis = "".join(f"<li>{n}</li>" for n in meta.get("parity_notes", [])) or """
<li><b>Earlier cutoff (KAN-6 AC4).</b> <code>--as-of-utc</code> is accepted and not applied, as in the legacy pipeline (PL_Master never passes AsOfUtc to the procs; the window is <code>MIN(dim.calendar)</code> + <code>WindowDays</code>). An earlier cutoff therefore yields identical output and reconcile passes. Meeting AC4's "earlier cutoff fails" would require a deliberate deviation from legacy, and filtering on <code>bucket_start_utc &lt; AsOfUtc</code> would also break parity, because the extract includes 2025-11-16 production-day buckets after 2025-11-17 00:00 UTC.</li>
<li><b>Canonical seed edge cases.</b> The extract is fixed, so the canonical seed can only carry edge cases that don't change its totals: zero-total buckets, a whole zero-total day (PLT06-L4, 2025-11-04, <code>yield_pct</code> NULL), 75-minute fall-back buckets, and <code>SKU-XX99</code> rows (dropped). Buckets that straddle 06:00 or a shift edge, spring-forward days, NULLs and duplicates are covered by the unit tests and the fuzz variants instead.</li>
<li><b>No seed tuning was needed.</b> The pre-existing seed already reproduces the extract exactly. The seed was not fitted to the snapshot.</li>
<li><b>Line terminator.</b> MANIFEST shows <code>bcp -r\\n</code>, but the committed extract uses CRLF. The byte-compare renders CRLF.</li>
<li><b>NULL counts</b> (impossible in legacy because the columns are NOT NULL) are dropped and counted, not failed. <b>INT overflow</b> raises, like SQL Server's arithmetic overflow.</li>
<li><b>Ambiguous / gap local times</b> follow java.time / zoneinfo fold=0, the documented AT TIME ZONE behaviour. This was not verified against a live SQL Server, and no current shift bound falls in such a window.</li>
<li>The ADF JSON in <code>lakehouse/adf/</code> was reviewed, not deployed. Nothing here ran on ADF or Databricks.</li>"""

    doc = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>{E(rep)} validation — {E(ticket)}</title>
<style>
:root{{--ok:#1f7a4d;--ko:#b3261e;--ink:#1d2433;--mute:#5b6577;--line:#e3e6ec;--bg:#f7f8fa}}
*{{box-sizing:border-box}}body{{margin:0;font:14px/1.45 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:var(--ink);background:var(--bg)}}
main{{max-width:1180px;margin:0 auto;padding:28px 24px 64px}}
h1{{font-size:22px;margin:0 0 4px}}h2{{font-size:16px;margin:32px 0 10px;padding-bottom:6px;border-bottom:1px solid var(--line)}}
.sub{{color:var(--mute);margin:0 0 18px}}code{{font:12px ui-monospace,Menlo,Consolas,monospace;background:#eef0f4;padding:1px 4px;border-radius:3px}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px}}
.card{{background:#fff;border:1px solid var(--line);border-radius:8px;padding:12px 14px}}
.card .k{{color:var(--mute);font-size:12px}}.card .v{{font-size:20px;font-weight:600;margin-top:2px}}
table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid var(--line);border-radius:8px;overflow:hidden}}
th,td{{text-align:left;padding:6px 9px;border-bottom:1px solid var(--line);vertical-align:top}}
th{{background:#f0f2f5;font-size:12px;color:var(--mute);font-weight:600;position:sticky;top:0}}
tr.bad td{{background:#fdf0ef}}td.f{{font-size:11.5px;color:var(--mute)}}
.b{{display:inline-block;padding:1px 7px;border-radius:10px;font-size:11px;font-weight:700;color:#fff}}
.b.ok{{background:var(--ok)}}.b.ko{{background:var(--ko)}}
.banner{{padding:12px 16px;border-radius:8px;color:#fff;font-weight:600;margin:14px 0}}
.banner.ok{{background:var(--ok)}}.banner.ko{{background:var(--ko)}}
video{{width:100%;border-radius:8px;border:1px solid var(--line);background:#000}}
.scroll{{max-height:560px;overflow:auto;border-radius:8px}}
input{{padding:6px 9px;border:1px solid var(--line);border-radius:6px;width:320px;margin-bottom:8px}}
ul li{{margin:4px 0}}
</style></head><body><main>
<h1>{E(pipeline)} → <code>mfg_lake.jobs.{E(rep)}</code> — validation</h1>
<p class="sub">{E(ticket)} · report <code>{E(rep)}</code> vs <code>legacy_snapshots/{E(table)}.csv</code> ·
AS_OF_UTC <code>{E(env['as_of_utc'])}</code> · commit <code>{E(env['git_commit'][:10])}</code> · generated {E(env['started_utc'])} UTC</p>
<div class="banner {'ok' if overall else 'ko'}">{'ALL VALIDATION PASSED' if overall else 'VALIDATION FAILED'} — {n_ok}/{len(checks)} checks pass</div>
<div class="cards">
<div class="card"><div class="k">Snapshot rows / lake rows</div><div class="v">{bc['snapshot_rows']} / {bc['lake_rows']}</div></div>
<div class="card"><div class="k">Keys matched / missing / extra</div><div class="v">{bc['matched_keys']} / {bc['missing_keys']} / {bc['extra_keys']}</div></div>
<div class="card"><div class="k">Mismatched rows</div><div class="v">{bc['mismatched_rows']}</div></div>
<div class="card"><div class="k">Byte-identical CSV</div><div class="v">{'yes' if bc['pass'] else 'no'}</div></div>
<div class="card"><div class="k">Fuzz variants passed</div><div class="v">{s['variants_passed']} / {s['variants_total']}</div></div>
<div class="card"><div class="k">Checker mutants caught</div><div class="v">{E(s['checker_mutants_caught'])}</div></div>
<div class="card"><div class="k">Unit tests</div><div class="v">{(str(tests['tests'] - tests['failures'] - tests['errors']) + ' / ' + str(tests['tests'])) if tests else 'n/a'}</div></div>
</div>

<h2>Recorded run</h2>
<p>Terminal recording of <code>tools/run_validation.sh</code>: seed, run, reconcile, pytest, <code>make ci</code>, fuzz.
Video: <a href="{E(a.video)}">{E(a.video)}</a> · asciinema cast: <a href="{E(a.cast)}">{E(a.cast)}</a> (<code>asciinema play {E(a.cast)}</code>)</p>
<video controls preload="metadata" src="{src}"></video>

<h2>1 · Schema / column contract vs <code>{E(table)}</code></h2>
<table><tr><th>column</th><th>legacy DDL (legacy/sql/schema/rpt.sql)</th><th>expected lake parquet type</th><th>in snapshot header</th></tr>{''.join(rows)}</table>
<p>Lake parquet schema check: {badge(schema_inv['pass'])} {E(schema_inv['detail'])}</p>

<h2>2 · Reconcile diff summary (canonical seed)</h2>
<p><code>tools/reconcile.py --report {E(rep)}</code>: keys <code>{E(', '.join(cfg['reports'][rep]['keys']))}</code>,
numeric tolerance <code>{cfg['numeric_tolerance']}</code> (absolute).
Byte-compare renders the lake output in the MANIFEST bcp format (ORDER BY key, NULL = empty, 2-dp decimals) with the extract's <code>{E(bc['line_terminator'])}</code> row terminator.
Snapshot {bc['snapshot_bytes']} bytes, sha256 <code>{E(bc['snapshot_sha256'])}</code>; lake CSV {bc['lake_bytes']} bytes, sha256 <code>{E(bc['lake_csv_sha256'])}</code>.</p>
<table><tr><th>control</th><th>detail</th><th>result</th></tr>{ctrl_rows}</table>

<h2>3 · Fuzz variants</h2>
<p>{fuzz_intro}</p>
<div class="scroll"><table><tr><th>#</th><th>rng seed</th><th>calendar</th><th>as_of_utc (ignored)</th><th>rows in</th><th>NULL rows</th><th>rows out</th><th>edge cases generated</th><th>result</th><th>invariant violated</th></tr>{''.join(vrows)}</table></div>

<h2>4 · Every check performed</h2>
<input id="q" placeholder="filter checks…" oninput="for(const r of document.querySelectorAll('#all tr+tr'))r.style.display=r.textContent.toLowerCase().includes(this.value.toLowerCase())?'':'none'">
<div class="scroll"><table id="all"><tr><th>#</th><th>section</th><th>check</th><th>result</th><th>detail</th></tr>{all_rows}</table></div>

<h2>5 · Parity notes and deviations</h2>
<ul>{parity_lis}
</ul>

<h2>6 · Environment</h2>
<table>{env_rows}</table>
</main></body></html>"""
    Path(a.out).write_text(doc)
    print(f"wrote {a.out} ({len(doc) // 1024} KiB, {n_ok}/{len(checks)} checks pass)")


if __name__ == "__main__":
    main()
