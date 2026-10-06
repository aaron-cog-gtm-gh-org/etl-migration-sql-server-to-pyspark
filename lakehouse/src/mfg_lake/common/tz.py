"""Time zone helpers.

mes feed timestamps are UTC; dim.plant.tz_name stores WINDOWS time zone
names (SQL Server AT TIME ZONE). Spark/JVM needs IANA names.

WINDOWS_TO_IANA follows the CLDR windowsZones "001" (primary) territory
mapping. Unknown names raise — never fall back silently to UTC.
"""
from typing import Iterable
from zoneinfo import ZoneInfo

WINDOWS_TO_IANA = {
    "UTC": "Etc/UTC",
    "GMT Standard Time": "Europe/London",
    "Greenwich Standard Time": "Atlantic/Reykjavik",
    "W. Europe Standard Time": "Europe/Berlin",
    "Romance Standard Time": "Europe/Paris",
    "Central Europe Standard Time": "Europe/Budapest",
    "Central European Standard Time": "Europe/Warsaw",
    "GTB Standard Time": "Europe/Bucharest",
    "FLE Standard Time": "Europe/Kiev",
    "Eastern Standard Time": "America/New_York",
    "Central Standard Time": "America/Chicago",
    "Mountain Standard Time": "America/Denver",
    "US Mountain Standard Time": "America/Phoenix",
    "Pacific Standard Time": "America/Los_Angeles",
    "Alaskan Standard Time": "America/Anchorage",
    "Hawaiian Standard Time": "Pacific/Honolulu",
    "Atlantic Standard Time": "America/Halifax",
    "Canada Central Standard Time": "America/Regina",
    "Central Standard Time (Mexico)": "America/Mexico_City",
    "Pacific Standard Time (Mexico)": "America/Tijuana",
    "E. South America Standard Time": "America/Sao_Paulo",
    "SA Pacific Standard Time": "America/Bogota",
    "Argentina Standard Time": "America/Argentina/Buenos_Aires",
    "South Africa Standard Time": "Africa/Johannesburg",
    "India Standard Time": "Asia/Kolkata",
    "China Standard Time": "Asia/Shanghai",
    "Singapore Standard Time": "Asia/Singapore",
    "Tokyo Standard Time": "Asia/Tokyo",
    "AUS Eastern Standard Time": "Australia/Sydney",
}


def windows_to_iana(windows_name: str) -> str:
    try:
        return WINDOWS_TO_IANA[windows_name]
    except KeyError:
        raise ValueError(f"no IANA mapping for Windows tz name {windows_name!r}") from None


def to_plant_local(windows_name: str) -> ZoneInfo:
    """Return a tzinfo for a Windows tz name, or raise ValueError."""
    return ZoneInfo(windows_to_iana(windows_name))


def require_mapped(windows_names: Iterable[str]) -> None:
    """Raise if any Windows tz name has no IANA mapping."""
    unmapped = sorted({n for n in windows_names if n not in WINDOWS_TO_IANA})
    if unmapped:
        raise ValueError(f"no IANA mapping for Windows tz name(s): {unmapped}")


def windows_to_iana_col(col):
    """Spark Column: Windows tz name column -> IANA name (NULL if unmapped)."""
    from itertools import chain

    from pyspark.sql import functions as F

    mapping = F.create_map(*[F.lit(x) for x in chain(*WINDOWS_TO_IANA.items())])
    return mapping[col]
