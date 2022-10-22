from datetime import date, time, datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/London")
UTC = ZoneInfo("UTC")


def sp_to_datetime(settlement_date: date, settlement_period: int) -> datetime:
    return (
        datetime.combine(settlement_date, time(0, 0, 0), tzinfo=TZ)
        + timedelta(minutes=(settlement_period - 1) * 30)
    ).astimezone(UTC)
