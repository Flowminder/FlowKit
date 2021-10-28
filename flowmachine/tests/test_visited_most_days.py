# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.visited_most_days import VisitedMostDays
from flowmachine.core import make_spatial_unit
import pytest


def test_visited_most_days_column_names(get_dataframe):
    """Test that column_names property is accurate"""
    vmd = VisitedMostDays(
        start_date="2016-01-01",
        end_date="2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=1),
    )
    assert get_dataframe(vmd).columns.tolist() == vmd.column_names


def test_vsites(get_dataframe):
    """
    VisitedMostDays() returns the correct locations.
    """

    vmd = VisitedMostDays(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("versioned-site")
    )
    df = get_dataframe(vmd)
    df.set_index("subscriber", inplace=True)

    assert "wzrXjw" == df.loc["yqQ6m96rp09dn510"].site_id
    assert "qvkp6J" == df.loc["zvaOknzKbEVD2eME"].site_id


def test_lon_lats(get_dataframe):
    """
    VisitedMostDays() has the correct values at the lon-lat spatial unit.
    """

    vmd = VisitedMostDays(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
    )
    df = get_dataframe(vmd)
    df.set_index("subscriber", inplace=True)

    assert pytest.approx(82.61895799084449) == float(df.loc["1QBlwRo4Kd5v3Ogz"].lon)
    assert pytest.approx(28.941925079951545) == float(df.loc["1QBlwRo4Kd5v3Ogz"].lat)


def test_most_frequent_admin(get_dataframe):
    """
    Test that the most frequent admin3 is correctly calculated.
    """
    vmd = VisitedMostDays(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("admin", level=3)
    )
    df = get_dataframe(vmd)
    # A few hand picked values
    df_set = df.set_index("subscriber")["pcod"]
    assert "524 4 12 62" == df_set["038OVABN11Ak4W5P"]
    assert "524 2 04 20" == df_set["zvaOknzKbEVD2eME"]
    assert df_set["ZYPxqVGLzlQy6l7n"] in ["524 4 12 62", "524 5 14 73"]
