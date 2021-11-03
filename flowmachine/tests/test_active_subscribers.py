import pytest
from flowmachine.features.subscriber.active_subscribers import ActiveSubscribers
from flowmachine.features.subscriber.unique_active_subscribers import (
    UniqueActiveSubscribers,
)
from datetime import date, datetime
from flowmachine.core.context import get_db
from pandas import DataFrame as df
from pandas.testing import assert_frame_equal


@pytest.fixture()
def active_sub_test_data(test_events_table):
    con = get_db().engine
    con.execute(
        """
        INSERT INTO events.test(msisdn, location_id, datetime)
        VALUES 
            ('AAAAAA', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-01 10:54:50.439203+00'),
            ('AAAAAA', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-01 11:54:50.439203+00'),
            ('AAAAAA', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-01 12:54:50.439203+00'),
            ('AAAAAA', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-04 10:54:50.439203+00'),
            ('BBBBBB', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-02 06:07:18.536049+00'),
            ('BBBBBB', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-02 06:07:18.536049+00'),
            ('BBBBBB', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-02 06:07:18.536049+00'),
            ('BBBBBB', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-04 06:07:18.536049+00'),
            ('CCCCCC', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-01 06:07:18.536049+00'),
            ('CCCCCC', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-02 06:07:18.536049+00'),
            ('CCCCCC', 'a9bb6802eda69ea4d030d1e585f6539d', '2016-01-03 06:07:18.536049+00');
    """
    )
    yield


def test_active_subscribers_one_day(active_sub_test_data):

    active_subscribers = ActiveSubscribers(
        start_date=date(year=2016, month=1, day=1),
        end_date=date(year=2016, month=1, day=2),
        active_hours=3,
        active_days=1,
        events_tables=["events.test"],
    )
    out = active_subscribers.get_dataframe()
    target = df.from_records(
        [("AAAAAA",)],
        columns=["subscriber"],
    )
    assert_frame_equal(out, target)


def test_active_subscribers_many_days(active_sub_test_data):

    active_subscribers = ActiveSubscribers(
        start_date=date(year=2016, month=1, day=1),
        end_date=date(year=2016, month=1, day=4),
        active_hours=1,
        active_days=3,
        events_tables=["events.test"],
    )
    out = active_subscribers.get_dataframe()
    sql = active_subscribers.get_query()
    print(out)
    target = df.from_records(
        [("CCCCCC",)],
        columns=["subscriber"],
    )
    assert_frame_equal(out, target)


def test_unique_active_subscribers(active_sub_test_data):
    unique_active_subscribers = UniqueActiveSubscribers(
        start_date=date(year=2016, month=1, day=1),
        end_date=date(year=2016, month=1, day=7),
        active_days=4,
        interval=7,
        events_tables=["events.test"],
    )
    target = df.from_records([("AAAAAA",), ("BBBBBB",)], columns=["subscriber"])
    out = unique_active_subscribers.get_dataframe()
    assert_frame_equal(out, target)
