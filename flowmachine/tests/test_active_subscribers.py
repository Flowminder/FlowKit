import pytest
from flowmachine.features.subscriber.active_subscribers import ActiveSubscribers
from flowmachine.features.subscriber.rolling_count_threshold_subscribers import (
    RollingCountThresholdSubscribers,
)
from datetime import date, datetime
from flowmachine.core.context import get_db
from pandas import DataFrame as df
from pandas.testing import assert_frame_equal


def test_active_subscribers_one_day(get_dataframe):

    active_subscribers = ActiveSubscribers(
        start_date=date(year=2016, month=1, day=1),
        end_date=date(year=2016, month=1, day=2),
        active_hours=3,
        active_days=1,
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
        end_date=date(year=2016, month=1, day=4),
        active_hours=1,
        active_days=3,
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
