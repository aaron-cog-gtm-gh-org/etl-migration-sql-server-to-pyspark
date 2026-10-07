"""Time zone helpers.

mes feed timestamps are UTC; dim.plant.tz_name stores WINDOWS time zone
names (SQL Server AT TIME ZONE). Spark/JVM needs IANA names.

The mapping follows the CLDR windowsZones "001" (primary) territory entry
for each Windows zone. It covers every tz_name in dim.plant plus the other
zones a Kimberly-Clark plant is likely to be added in.
"""
from datetime import tzinfo
from zoneinfo import ZoneInfo

WINDOWS_TO_IANA = {
    # zones present in dim.plant
    "Central Standard Time": "America/Chicago",
    "Eastern Standard Time": "America/New_York",
    "Pacific Standard Time": "America/Los_Angeles",
    "Central Standard Time (Mexico)": "America/Mexico_City",
    "E. South America Standard Time": "America/Sao_Paulo",
    "GMT Standard Time": "Europe/London",
    # other common plant zones
    "UTC": "Etc/UTC",
    "Mountain Standard Time": "America/Denver",
    "US Mountain Standard Time": "America/Phoenix",
    "Alaskan Standard Time": "America/Anchorage",
    "Hawaiian Standard Time": "Pacific/Honolulu",
    "Atlantic Standard Time": "America/Halifax",
    "Canada Central Standard Time": "America/Regina",
    "SA Pacific Standard Time": "America/Bogota",
    "Argentina Standard Time": "America/Buenos_Aires",
    "Pacific SA Standard Time": "America/Santiago",
    "W. Europe Standard Time": "Europe/Berlin",
    "Romance Standard Time": "Europe/Paris",
    "Central Europe Standard Time": "Europe/Budapest",
    "Central European Standard Time": "Europe/Warsaw",
    "GTB Standard Time": "Europe/Bucharest",
    "FLE Standard Time": "Europe/Kiev",
    "Turkey Standard Time": "Europe/Istanbul",
    "Russian Standard Time": "Europe/Moscow",
    "South Africa Standard Time": "Africa/Johannesburg",
    "E. Africa Standard Time": "Africa/Nairobi",
    "Arabian Standard Time": "Asia/Dubai",
    "India Standard Time": "Asia/Calcutta",
    "SE Asia Standard Time": "Asia/Bangkok",
    "Singapore Standard Time": "Asia/Singapore",
    "China Standard Time": "Asia/Shanghai",
    "Taipei Standard Time": "Asia/Taipei",
    "Korea Standard Time": "Asia/Seoul",
    "Tokyo Standard Time": "Asia/Tokyo",
    "AUS Eastern Standard Time": "Australia/Sydney",
    "New Zealand Standard Time": "Pacific/Auckland",
}


def windows_to_iana(windows_name: str) -> str:
    """IANA zone id for a Windows tz name; KeyError if unmapped."""
    key = windows_name.strip() if windows_name is not None else windows_name
    try:
        return WINDOWS_TO_IANA[key]
    except KeyError:
        raise KeyError(f"no IANA mapping for Windows time zone {windows_name!r}") from None


def to_plant_local(windows_name: str) -> tzinfo:
    """Return a tzinfo for a Windows tz name, or raise KeyError."""
    return ZoneInfo(windows_to_iana(windows_name))
