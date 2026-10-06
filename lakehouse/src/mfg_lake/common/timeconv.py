"""Timestamp conversions matching SQL Server DATETIME2 and DATEDIFF semantics."""
import re
from datetime import datetime, timedelta, timezone

from pyspark.sql import Column
from pyspark.sql import functions as F

_AS_OF_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?\s*(Z|[+-]\d{2}:?\d{2})?$")


def parse_as_of_utc(value: str) -> str:
    """Normalise --as-of-utc to 'YYYY-MM-DD HH:MM:SS' (UTC, whole seconds).

    Accepts the Makefile form ('2025-11-17 00:00:00') and the ADF
    @trigger().scheduledTime form ('2025-11-17T06:30:00.0000000Z'). Fractional
    seconds round half-up, as SQL Server does converting a string to the
    proc's DATETIME2(0) @AsOfUtc parameter.
    """
    m = _AS_OF_RE.match(value.strip())
    if not m:
        raise ValueError(f"unparseable --as-of-utc {value!r}")
    day, hms, frac, off = m.groups()
    dt = datetime.strptime(f"{day} {hms}", "%Y-%m-%d %H:%M:%S")
    if frac and int(frac[0]) >= 5:
        dt += timedelta(seconds=1)
    if off and off != "Z":
        sign = 1 if off[0] == "+" else -1
        hh, mm = int(off[1:3]), int(off[-2:])
        dt = dt.replace(tzinfo=timezone(sign * timedelta(hours=hh, minutes=mm)))
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def ts_seconds(col) -> Column:
    """Timestamp truncated to whole seconds (DATETIME2(0))."""
    c = F.col(col) if isinstance(col, str) else col
    return F.date_trunc("second", c.cast("timestamp"))


def to_local(utc_col, iana_col) -> Column:
    """CAST(x AT TIME ZONE 'UTC' AT TIME ZONE tz AS DATETIME2(0))."""
    return F.date_trunc("second", F.from_utc_timestamp(utc_col, iana_col))


def datediff_minute(start, end) -> Column:
    """T-SQL DATEDIFF(MINUTE, start, end): minute boundaries crossed.

    floor(epoch(end)/60) - floor(epoch(start)/60) -- NOT elapsed seconds / 60.
    Operates on wall-clock values; relies on spark.sql.session.timeZone=UTC.
    """
    s = F.col(start) if isinstance(start, str) else start
    e = F.col(end) if isinstance(end, str) else end
    return (F.floor(F.unix_timestamp(e) / 60)
            - F.floor(F.unix_timestamp(s) / 60)).cast("int")
