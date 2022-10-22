from settlement_periods import sp_to_datetime, TZ
from datetime import date, datetime


def test_settlement_periods():
    d = date(2022, 10, 9)
    assert sp_to_datetime(d, 1) == datetime(2022, 10, 9, 0, 0, 0, tzinfo=TZ)
    assert sp_to_datetime(d, 48) == datetime(2022, 10, 9, 23, 30, 0, tzinfo=TZ)
    assert sp_to_datetime(date.fromisoformat("2022-10-09"), 1) == datetime(
        2022, 10, 9, 0, 0, 0, tzinfo=TZ
    )
