# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime as dt

import pytest

from flowmachine.core.date_range import DateRange


def test_start_and_end_date():
    """
    DateRange knows its start and end date,
    """
    dp = DateRange(start_date="2016-01-01", end_date="2016-01-03")
    assert dp.start_date == dt.datetime(2016, 1, 1, 0, 0, 0)
    assert dp.end_date == dt.datetime(2016, 1, 3, 0, 0, 0)
    assert dp.start_date_as_str == "2016-01-01 00:00:00"
    assert dp.end_date_as_str == "2016-01-03 00:00:00"


def test_one_day_past_end_date():
    """
    DateRange knows the date after its end date.
    """
    dp = DateRange(start_date="2016-01-07", end_date="2016-01-14")
    assert dp.one_day_past_end_date == dt.datetime(2016, 1, 15, 0, 0, 0)
    assert dp.one_day_past_end_date_as_str == "2016-01-15 00:00:00"

    dp = DateRange(
        start_date=dt.date(2016, 4, 12), end_date=dt.datetime(2016, 5, 31, 0, 0, 0)
    )
    assert dp.one_day_past_end_date == dt.datetime(2016, 6, 1, 0, 0, 0)
    assert dp.one_day_past_end_date_as_str == "2016-06-01 00:00:00"


@pytest.mark.parametrize(
    "expected_error_type, start_date, end_date",
    [
        (ValueError, "9999-99-99", "2016-01-03"),
        (ValueError, "2016-01-01", "9999-99-99"),
        (TypeError, 42, "2016-01-03"),
        (TypeError, "2016-01-01", 42),
    ],
)
def test_invalid_start_or_end_dates(expected_error_type, start_date, end_date):
    """
    DateRange knows the date after its end date.
    """
    with pytest.raises(expected_error_type):
        DateRange(start_date=start_date, end_date=end_date)
