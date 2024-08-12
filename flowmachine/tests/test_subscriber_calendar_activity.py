# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the class flowmachine.TotalActivePeriodsSubscriber
"""
import datetime
import pytest

from flowmachine.features.subscriber.calendar_activity import CalendarActivity


def test_certain_results(get_dataframe):
    """
    calendar activity gives correct results.
    """
    tap = CalendarActivity("2016-01-01", 3, 1)
    # This subscriber should have only 2 active time periods
    subscriber_with_2 = "rmb4PG9gx1ZqBE01"
    # and this one three
    subscriber_with_3 = "dXogRAnyg1Q9lE3J"

    df = get_dataframe(tap).set_index("subscriber")
    assert df.loc[subscriber_with_2].value == [
        datetime.datetime(2016, 1, 1, 0, 0),
        datetime.datetime(2016, 1, 3, 0, 0),
    ]
    assert df.loc[subscriber_with_3].value == [
        datetime.datetime(2016, 1, 1, 0, 0),
        datetime.datetime(2016, 1, 2, 0, 0),
        datetime.datetime(2016, 1, 3, 0, 0),
    ]


def test_non_standard_units(get_dataframe):
    """
    calendar activity is able to handle a period_unit other
    than the default 'days'.
    """

    df = (
        CalendarActivity("2016-01-01", 5, period_unit="hours")
        .get_dataframe()
        .set_index("subscriber")
    )

    assert df.loc["VkzMxYjv7mYn53oK"].value == [
        datetime.datetime(2016, 1, 1, 0, 0),
        datetime.datetime(2016, 1, 1, 2, 0),
        datetime.datetime(2016, 1, 1, 4, 0),
    ]
