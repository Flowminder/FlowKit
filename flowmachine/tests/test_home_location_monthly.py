from flowmachine.features.subscriber.home_location_monthly import HomeLocationMonthly
from flowmachine.core.spatial_unit import AnySpatialUnit, AdminSpatialUnit
from flowmachine.core.query import Query
from flowmachine.core.context import get_db
import pytest
from pandas import DataFrame as df
from pandas.testing import assert_frame_equal


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
        spatial_unit=AdminSpatialUnit(level=3),
        home_this_month=3,
        home_last_month=2,
        events_tables=["events.test"],
        modal_lookback=10,
    )

    out = home_location_monthly.get_dataframe()
    sql = home_location_monthly.get_query()
    target = df.from_records(
        [
            ("AAAAAA", "NPL.1.3.2_1"),
            ("BBBBBB", "NPL.4.3.2_1"),
            ("CCCCCC", "unknown"),
        ],
        columns=["subscriber", "location"],
    )
    assert_frame_equal(out, target)


def test_home_location_monthly_chained(test_events_table):

    home_location_monthly = HomeLocationMonthly(
        window_start="2016-01-07",
        window_stop="2016-01-10",
        spatial_unit=AdminSpatialUnit(level=3),
        home_this_month=3,
        home_last_month=2,
        events_tables=["events.test"],
    )

    home_location_next_month = HomeLocationMonthly(
        window_start="2016-01-11",
        window_stop="2016-01-14",
        spatial_unit=AdminSpatialUnit(level=3),
        home_this_month=3,
        home_last_month=2,
        ref_location=home_location_monthly,
        events_tables=["events.test"],
    )

    assert home_location_next_month.ref_location.window_start == "2016-01-07"
    out = home_location_next_month.get_dataframe()
    assert out
