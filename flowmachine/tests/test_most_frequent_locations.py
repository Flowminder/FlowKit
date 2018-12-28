# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.features import MostFrequentLocation
from flowmachine.features.subscriber.daily_location import locate_subscribers


def test_most_frequent_locations_column_names(get_dataframe, exemplar_level_param):
    """
    MostFrequentLocations().get_dataframe() returns a dataframe.
    """
    mfl = MostFrequentLocation("2016-01-01", "2016-01-02", **exemplar_level_param)
    df = get_dataframe(mfl)
    assert df.columns.tolist() == mfl.column_names


def test_vsites(get_dataframe):
    """
    MostFrequentLocation() returns the correct locations.
    """

    mfl = MostFrequentLocation("2016-01-01", "2016-01-02", level="versioned-site")
    df = get_dataframe(mfl)
    df.set_index("subscriber", inplace=True)

    assert "wzrXjw" == df.ix["yqQ6m96rp09dn510"].site_id
    assert "qvkp6J" == df.ix["zvaOknzKbEVD2eME"].site_id


def test_lat_lons(get_dataframe):
    """
    MostFrequentLocations() has the correct values at the lat-lon level.
    """

    mfl = MostFrequentLocation("2016-01-01", "2016-01-02", level="lat-lon")
    df = get_dataframe(mfl)
    df.set_index("subscriber", inplace=True)

    assert pytest.approx(28.941925079951545) == float(df.ix["1QBlwRo4Kd5v3Ogz"].lat)
    assert pytest.approx(82.61895799084449) == float(df.ix["1QBlwRo4Kd5v3Ogz"].lon)


def test_most_fequent_admin(get_dataframe):
    """
    Test that the most frequent admin3 is correctly calculated.
    """

    mfl = locate_subscribers(
        "2016-01-01", "2016-01-02", level="admin3", method="most-common"
    )
    df = get_dataframe(mfl)
    # A few hand picked values
    df_set = df.set_index("subscriber")["pcod"]
    assert "524 4 12 62" == df_set["0gmvwzMAYbz5We1E"]
    assert "524 4 10 52" == df_set["1QBlwRo4Kd5v3Ogz"]
    assert "524 3 09 50" == df_set["2Dq97XmPqvL6noGk"]
