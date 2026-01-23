import asyncio
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))


def kst_now() -> datetime:
    return datetime.now(KST)


def next_kst_datetime(hour: int, minute: int, *, base: datetime | None = None) -> datetime:
    base = base or kst_now()
    candidate = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= base:
        candidate += timedelta(days=1)
    return candidate


async def sleep_until(dt: datetime):
    while True:
        now = kst_now()
        sec = (dt - now).total_seconds()
        if sec <= 0:
            return
        await asyncio.sleep(min(sec, 60.0))


def is_weekend(dt: datetime) -> bool:
    return dt.weekday() >= 5  # Sat/Sun
