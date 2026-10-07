"""Windows -> IANA time zone mapping (dim.plant.tz_name is a Windows name)."""
import csv
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from mfg_lake.common.tz import to_plant_local, windows_to_iana

ROOT = Path(__file__).resolve().parent.parent


def _plant_tz_names():
    with (ROOT / "data" / "raw" / "dim.plant.csv").open() as f:
        return sorted({r["tz_name"] for r in csv.DictReader(f)})


@pytest.mark.parametrize("win", _plant_tz_names())
def test_every_plant_tz_is_mapped(win):
    assert ZoneInfo(windows_to_iana(win))


@pytest.mark.parametrize("win,iana", [
    ("Central Standard Time", "America/Chicago"),
    ("Eastern Standard Time", "America/New_York"),
    ("Pacific Standard Time", "America/Los_Angeles"),
    ("Central Standard Time (Mexico)", "America/Mexico_City"),
    ("E. South America Standard Time", "America/Sao_Paulo"),
    ("GMT Standard Time", "Europe/London"),
])
def test_plant_mappings(win, iana):
    assert windows_to_iana(win) == iana


def test_mapping_is_whitespace_tolerant():
    # SQL Server CHAR/VARCHAR feeds can carry trailing padding
    assert windows_to_iana("GMT Standard Time  ") == "Europe/London"


def test_unknown_windows_name_raises():
    with pytest.raises(KeyError):
        windows_to_iana("Atlantis Standard Time")


def test_gmt_standard_time_is_london_not_utc():
    # 'GMT Standard Time' observes BST; a naive GMT->UTC mapping is wrong
    tz = to_plant_local("GMT Standard Time")
    summer = datetime(2025, 10, 20, 12, tzinfo=timezone.utc).astimezone(tz)
    assert summer.utcoffset().total_seconds() == 3600


@pytest.mark.parametrize("win,utc_dt,expected_hours", [
    # US fall-back 2025-11-02: CDT before, CST after
    ("Central Standard Time", datetime(2025, 11, 2, 6, 0), -5),
    ("Central Standard Time", datetime(2025, 11, 2, 8, 0), -6),
    # Mexico abolished DST in 2022 -> UTC-6 all year
    ("Central Standard Time (Mexico)", datetime(2025, 10, 20, 12), -6),
    ("Central Standard Time (Mexico)", datetime(2025, 11, 10, 12), -6),
    # Brazil abolished DST in 2019 -> UTC-3 all year
    ("E. South America Standard Time", datetime(2025, 11, 10, 12), -3),
])
def test_to_plant_local_offsets(win, utc_dt, expected_hours):
    local = utc_dt.replace(tzinfo=timezone.utc).astimezone(to_plant_local(win))
    assert local.utcoffset().total_seconds() == expected_hours * 3600
