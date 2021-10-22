import pytest
from flowmachine.features.subscriber.active_subscribers import ActiveSubscribers
from flowmachine.features.subscriber.unique_active_subscribers import (
    UniqueActiveSubscribers,
)
from datetime import date


def test_active_subscribers():
    active_subscribers = ActiveSubscribers(
        start_date=date(year=2016, month=1, day=7),
        end_date=date(year=2016, month=2, day=1),
        interval=7,
        active_days=4,
    )
    # test from flowdb_syntheticdata
    assert next(iter(active_subscribers)) == [
        "2016-01-07",
        "000243e7eece4fffa71428f5aa17994b",
    ]


def test_unique_active_subscribers():
    unique_active_subscribers = UniqueActiveSubscribers(
        start_date=date(year=2016, month=1, day=7),
        end_date=date(year=2016, month=2, day=1),
        interval=7,
        active_days=4,
    )
    assert len(list(iter(unique_active_subscribers))) == 500
