# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features import HomeLocation, daily_location
from flowmachine.features.subscriber.daily_location import locate_subscribers
from flowmachine.utils import list_of_dates


def test_can_be_aggregated_admin3(get_dataframe):
    """
    Query can be aggregated to a spatial level with admin3 data.
    """
    mfl = locate_subscribers(
        "2016-01-01", "2016-01-02", level="admin3", method="most-common"
    )
    agg = mfl.aggregate()
    df = get_dataframe(agg)
    assert ["name", "total"] == list(df.columns)


def test_can_be_aggregated_latlong(get_dataframe):
    """
    Query can be aggregated to a spatial level with lat-lon data.
    """
    hl = HomeLocation(
        *[
            daily_location(d, level="lat-lon", method="last")
            for d in list_of_dates("2016-01-01", "2016-01-03")
        ]
    )
    agg = hl.aggregate()
    df = get_dataframe(agg)
    assert ["lat", "lon", "total"] == list(df.columns)
