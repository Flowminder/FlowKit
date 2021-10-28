import pytest
from flowmachine.features.subscriber.active_subscribers import ActiveSubscribers
from flowmachine.features.subscriber.unique_active_subscribers import (
    UniqueActiveSubscribers,
)
from datetime import date, datetime
from flowmachine.core.context import get_db


@pytest.fixture()
def active_sub_test_data(test_events_table):
    con = get_db().engine
    con.execute(
        """
        INSERT INTO events.test(msisdn, location_id, datetime)
        VALUES 
            ('AAAAAA', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-01 10:54:50.439203+00'),
            ('AAAAAA', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-02 11:54:50.439203+00'),
            ('AAAAAA', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-03 12:54:50.439203+00'),
            ('AAAAAA', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-04 10:54:50.439203+00'),
            ('BBBBBB', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-01 06:07:18.536049+00'),
            ('BBBBBB', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-02 06:07:18.536049+00'),
            ('BBBBBB', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-03 06:07:18.536049+00'),
            ('BBBBBB', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-04 06:07:18.536049+00'),
            ('CCCCCC', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-01 06:07:18.536049+00'),
            ('CCCCCC', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-02 06:07:18.536049+00'),
            ('CCCCCC', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-03 06:07:18.536049+00');
    """
    )
    yield


def test_active_subscribers(active_sub_test_data):

    active_subscribers = ActiveSubscribers(
        start_date=date(year=2016, month=1, day=1),
        end_date=date(year=2016, month=2, day=7),
        active_days=4,
        interval=7,
        events_tables=["events.test"],
    )
    # test from flowdb_syntheticdata
    out = list(iter(active_subscribers))
    assert out == [(date(2016, 1, 4), "AAAAAA"), (date(2016, 1, 4), "BBBBBB")]


def test_unique_active_subscribers(active_sub_test_data):
    unique_active_subscribers = UniqueActiveSubscribers(
        start_date=date(year=2016, month=1, day=1),
        end_date=date(year=2016, month=1, day=7),
        active_days=4,
        interval=7,
        events_tables=["events.test"],
    )
    out = list(iter(unique_active_subscribers))
    assert out == [("AAAAAA",), ("BBBBBB",)]
