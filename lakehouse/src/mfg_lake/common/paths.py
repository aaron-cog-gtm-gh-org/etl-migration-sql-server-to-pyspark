"""Lake path resolution.

The ADF estate writes curated output to
    abfss://curated@nwhmfglake.dfs.core.windows.net/manufacturing/<report>
That abfss URI is the contract a converted job must write to. Locally it
resolves to $LAKE_ROOT/<ns>/curated/<report> (default lake root ./out).

Raw seed feeds resolve to <repo>/data/raw/<schema>.<table>.csv via raw_csv().
"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
LAKE_ROOT = Path(os.environ.get("LAKE_ROOT", REPO_ROOT / "out"))

_ACCOUNT = "nwhmfglake"
_CONTAINER = "curated"
_PREFIX = "manufacturing"


def abfss_uri(report: str) -> str:
    """Canonical ADLS path for a curated report."""
    return (f"abfss://{_CONTAINER}@{_ACCOUNT}.dfs.core.windows.net/"
            f"{_PREFIX}/{report}")


def curated_dir(report: str, ns: str) -> Path:
    """Local path that abfss_uri(report) resolves to for namespace ns."""
    return LAKE_ROOT / ns / _CONTAINER / _PREFIX / report


def reconciliation_dir(ns: str) -> Path:
    return LAKE_ROOT / ns / "reconciliation"


def raw_csv(schema_table: str) -> Path:
    """<repo>/data/raw/<schema>.<table>.csv"""
    return REPO_ROOT / "data" / "raw" / f"{schema_table}.csv"
