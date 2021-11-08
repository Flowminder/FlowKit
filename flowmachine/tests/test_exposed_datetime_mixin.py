from flowmachine.core.mixins import ExposedDatetimeMixin
from datetime import datetime


class DummyDatetime(ExposedDatetimeMixin):
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date


def test_exposed_datetime_mixin():
    dummy_datetime = DummyDatetime(
        start_date="2021-01-02", end_date="2021-01-03 12:00:01"
    )
    assert dummy_datetime.start_date == "2021-01-02 00:00:00"
    assert dummy_datetime._end_dt == datetime(
        year=2021, month=1, day=3, hour=12, minute=0, second=1
    )
