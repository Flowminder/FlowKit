# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features import LocationVisits, daily_location, DayTrajectories
from flowmachine.utils import list_of_dates


def test_column_names_location_visits(exemplar_level_param):
    """ Test that column_names property matches head(0) for LocationVisits"""
    lv = LocationVisits(
        DayTrajectories(daily_location("2016-01-01", **exemplar_level_param))
    )
    assert lv.head(0).columns.tolist() == lv.column_names


def test_dl_count_sum_equal_or_less_than_period(get_dataframe):
    """
    Sum of LocationVisits per subscriber should not be more than total
    number of days between 'start_date' and 'stop_date'
    """
    # test 1
    days = 7
    start_date = "2016-01-01"
    stop_date = "2016-01-07"
    lv = LocationVisits(
        DayTrajectories(
            *[
                daily_location(d, level="admin3", method="last")
                for d in list_of_dates(start_date, stop_date)
            ]
        )
    )
    df = get_dataframe(lv)
    assert df[df["subscriber"] == df.iloc[0, 0]]["dl_count"].sum() <= days
    # test 2
    days = 3
    start_date = "2016-01-01"
    stop_date = "2016-01-03"
    lv = LocationVisits(
        DayTrajectories(
            *[
                daily_location(d, level="admin3", method="last")
                for d in list_of_dates(start_date, stop_date)
            ]
        )
    )
    df = get_dataframe(lv)
    assert df[df["subscriber"] == df.iloc[0, 0]]["dl_count"].sum() <= days
