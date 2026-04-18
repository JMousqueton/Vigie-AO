"""
Utility helpers shared across the application.
"""
from datetime import datetime, timezone


def utc_now() -> datetime:
    """
    Return the current UTC time as a **timezone-naive** datetime.

    We store and compare datetimes as naive UTC throughout (SQLite has no
    native TZ support).  datetime.utcnow() is deprecated since Python 3.12;
    this wrapper uses the recommended datetime.now(timezone.utc) internally
    and strips the tzinfo so existing SQLAlchemy filter comparisons keep
    working without modification.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)
