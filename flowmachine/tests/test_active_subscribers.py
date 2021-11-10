# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pytest
from flowmachine.features.subscriber.active_subscribers import ActiveSubscribers
from datetime import date, datetime
from flowmachine.core.context import get_db
from pandas import DataFrame as df
from pandas.testing import assert_frame_equal


def test_active_subscribers_one_day(get_dataframe):

    active_subscribers = ActiveSubscribers(
        start_date=date(year=2016, month=1, day=1),
        minor_period_length=1,
        minor_periods_per_major_period=24,
        total_major_periods=1,
        minor_period_threshold=3,
        major_period_threshold=1,
        tables=["events.calls"],
    )
    out = get_dataframe(active_subscribers).iloc[0:5]
    print(out)
    target = df.from_records(
        [
            ["038OVABN11Ak4W5P"],
            ["0DB8zw67E9mZAPK2"],
            ["0gmvwzMAYbz5We1E"],
            ["0MQ4RYeKn7lryxGa"],
            ["0W71ObElrz5VkdZw"],
        ],
        columns=["subscriber"],
    )
    assert_frame_equal(out, target)


def test_active_subscribers_many_days(get_dataframe):

    active_subscribers = ActiveSubscribers(
        start_date=date(year=2016, month=1, day=1),
        minor_period_length=1,
        minor_periods_per_major_period=24,
        total_major_periods=4,
        minor_period_threshold=1,
        major_period_threshold=3,
        tables=["events.calls"],
    )
    out = get_dataframe(active_subscribers).iloc[0:5]
    print(out)
    target = df.from_records(
        [
            ["038OVABN11Ak4W5P"],
            ["09NrjaNNvDanD8pk"],
            ["0ayZGYEQrqYlKw6g"],
            ["0DB8zw67E9mZAPK2"],
            ["0Gl95NRLjW2aw8pW"],
        ],
        columns=["subscriber"],
    )
    assert_frame_equal(out, target)


def test_active_subscribers_custom_period(get_dataframe):
    # Test for subscribers that are active in at least two ten minute intervals in half an hour, at least
    # three times across two hours
    active_subscribers = ActiveSubscribers(
        start_date=datetime(year=2016, month=1, day=1, hour=20),
        minor_period_length=10,
        minor_periods_per_major_period=3,
        total_major_periods=4,
        minor_period_threshold=2,
        major_period_threshold=3,
        period_unit="minutes",
        tables=["events.calls"],
    )
    assert len(active_subscribers.period_queries) == 4
    assert active_subscribers.period_queries[2].start == "2016-01-01 21:00:00"
