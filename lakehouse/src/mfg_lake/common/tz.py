"""Time zone helpers.

mes feed timestamps are UTC; dim.plant.tz_name stores WINDOWS time zone
names (SQL Server AT TIME ZONE). Spark/JVM needs IANA names.

Left intentionally unimplemented for now — conversions that need local
time must provide the Windows -> IANA mapping themselves.
"""


def windows_to_iana(windows_name: str) -> str:
    raise NotImplementedError("Windows->IANA mapping not implemented yet")


def to_plant_local(windows_name: str):
    """Return a tzinfo for a Windows tz name, or raise."""
    raise NotImplementedError("Windows->IANA mapping not implemented yet")
