"""Time zone helpers.

mes feed timestamps are UTC; dim.plant.tz_name stores WINDOWS time zone
names (SQL Server AT TIME ZONE). Spark/JVM needs IANA names.

Mapping covers the plant fleet; extend dim.plant additions here.
"""
from zoneinfo import ZoneInfo

WIN_TO_IANA = {
    "Central Standard Time": "America/Chicago",
    "Eastern Standard Time": "America/New_York",
    "Pacific Standard Time": "America/Los_Angeles",
    "Central Standard Time (Mexico)": "America/Mexico_City",
    "E. South America Standard Time": "America/Sao_Paulo",
    "GMT Standard Time": "Europe/London",
}


def windows_to_iana(windows_name: str) -> str:
    try:
        return WIN_TO_IANA[windows_name]
    except KeyError:
        raise ValueError(f"no IANA mapping for Windows tz {windows_name!r}")


def to_plant_local_tzinfo(windows_name: str) -> ZoneInfo:
    return ZoneInfo(windows_to_iana(windows_name))


def local_to_utc(local_dt, windows_name: str):
    """Naive local wall time -> naive UTC.

    Ambiguous fall-back times take the pre-transition (daylight) offset;
    spring-forward gap times shift forward on the pre-transition offset
    (zoneinfo fold=0 — matches AT TIME ZONE).
    """
    return (local_dt.replace(tzinfo=to_plant_local_tzinfo(windows_name), fold=0)
            .astimezone(ZoneInfo("UTC")).replace(tzinfo=None))
