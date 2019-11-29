# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    ModalLocation,
    daily_location,
    SubscriberHandsetCharacteristic,
)
from flowmachine.features.location.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)
from flowmachine.features.subscriber.daily_location import locate_subscribers
from flowmachine.utils import list_of_dates


def test_can_be_aggregated_admin3(get_dataframe):
    """
    Query can be aggregated to a spatial level with admin3 data.
    """
    mfl = locate_subscribers(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=3),
        method="most-common",
    )
    agg = mfl.aggregate()
    df = get_dataframe(agg)
    assert ["pcod", "value"] == list(df.columns)


def test_can_be_aggregated_lon_lat(get_dataframe):
    """
    Query can be aggregated to a spatial level with lon-lat data.
    """
    hl = ModalLocation(
        *[
            daily_location(d, spatial_unit=make_spatial_unit("lon-lat"), method="last")
            for d in list_of_dates("2016-01-01", "2016-01-03")
        ]
    )
    agg = hl.aggregate()
    df = get_dataframe(agg)
    assert ["lon", "lat", "value"] == list(df.columns)


def test_can_be_aggregated_admin3_distribution(get_dataframe):
    """
    Categorical queries can be aggregated to a spatial level with 'distribution' method.
    """
    locations = locate_subscribers(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=3),
        method="most-common",
    )
    metric = SubscriberHandsetCharacteristic(
        "2016-01-01", "2016-01-02", characteristic="hnd_type"
    )
    agg = JoinedSpatialAggregate(metric=metric, locations=locations, method="distr")
    df = get_dataframe(agg)
    assert ["pcod", "metric", "key", "value"] == list(df.columns)
    assert all(df[df.metric == "value"].groupby("pcod").sum() == 1.0)
