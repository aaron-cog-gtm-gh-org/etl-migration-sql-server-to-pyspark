from datetime import datetime

from mfg_lake.common.tz import local_to_utc, windows_to_iana


def test_windows_to_iana():
    assert windows_to_iana("Central Standard Time") == "America/Chicago"


def test_local_to_utc():
    # Chicago CDT (UTC-5) in October
    assert local_to_utc(datetime(2025, 10, 20, 8, 0),
                        "Central Standard Time") == datetime(2025, 10, 20, 13, 0)


def test_local_to_utc_ambiguous_pretransition():
    # 2025-11-02 01:30 is ambiguous in Chicago; pre-transition offset wins
    assert local_to_utc(datetime(2025, 11, 2, 1, 30),
                        "Central Standard Time") == datetime(2025, 11, 2, 6, 30)
