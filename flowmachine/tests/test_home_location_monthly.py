from flowmachine.features.subscriber.home_location_monthly import HomeLocationMonthly
from flowmachine.core.spatial_unit import AnySpatialUnit, AdminSpatialUnit
from flowmachine.core.query import Query
from flowmachine.core.context import get_db
import pytest


@pytest.fixture()
def home_loc_test_data(test_events_table):

    # Test data notes:
    # a8eb53475ccd0c0ed0d73a9106dd7f25 corresponds to NPL.1.3.2_1
    # a9bb6802eda69ea4d030d1e585f6539d corresponds to NPL.4.3.2_1
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
            ('CCCCCC', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-03 06:07:18.536049+00'),
            ('CCCCCC', 'a8eb53475ccd0c0ed0d73a9106dd7f25', '2016-01-04 06:07:18.536049+00');
    """
    )
    yield


def test_home_location_monthly_single(home_loc_test_data):

    home_location_monthly = HomeLocationMonthly(
        window_start="2016-01-01",
        window_stop="2016-01-07",
        agg_unit=AdminSpatialUnit(level=3),
        unknown_threshold=3,
        known_threshold=2,
        events_tables=["events.test"],
        modal_lookback=10,
    )

    out = list(iter(home_location_monthly))
    sql = home_location_monthly.get_query()
    assert out == [
        ("AAAAAA", "NPL.1.3.2_1"),
        ("BBBBBB", "NPL.4.3.2_1"),
        ("CCCCCC", "unknown"),
    ]


def test_home_location_monthly_chained(test_events_table):

    home_location_monthly = HomeLocationMonthly(
        window_start="2016-01-07",
        window_stop="2016-01-10",
        agg_unit=AdminSpatialUnit(level=3),
        unknown_threshold=3,
        known_threshold=2,
        events_tables=["events.test"],
    )

    home_location_next_month = HomeLocationMonthly(
        window_start="2016-01-11",
        window_stop="2016-01-14",
        agg_unit=AdminSpatialUnit(level=3),
        unknown_threshold=3,
        known_threshold=2,
        ref_location=home_location_monthly,
        events_tables=["events.test"],
    )

    assert home_location_next_month.ref_location.window_start == "2016-01-07"
    out = list(iter(home_location_next_month))
    assert out
