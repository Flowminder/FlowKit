# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from flowmachine.core.mixins import ExposedDatetimeMixin
from datetime import datetime, date


class DummyDatetime(ExposedDatetimeMixin):
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date


@pytest.mark.parametrize(
    "start_date",
    [
        "2021-01-01",
        "2021-01-01 00:00:00",
        date(year=2021, month=1, day=1),
        datetime(year=2021, month=1, day=1, hour=0, minute=0, second=0),
    ],
)
@pytest.mark.parametrize(
    "end_date",
    [
        "2021-01-03",
        "2021-01-03 00:00:00",
        date(year=2021, month=1, day=3),
        datetime(year=2021, month=1, day=3, hour=0, minute=0, second=0),
    ],
)
def test_exposed_datetime_mixin(start_date, end_date):
    dummy_datetime = DummyDatetime(start_date=start_date, end_date=end_date)
    assert dummy_datetime.start_date == "2021-01-01 00:00:00"
    assert dummy_datetime.end_date == "2021-01-03 00:00:00"
    assert dummy_datetime._start_dt == datetime(
        year=2021, month=1, day=1, hour=0, minute=0, second=0
    )
    assert dummy_datetime._end_dt == datetime(
        year=2021, month=1, day=3, hour=0, minute=0, second=0
    )
