# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import numpy as np

from flowmachine.core.spatial_unit import make_spatial_unit, CellSpatialUnit
from flowmachine.core.custom_query import CustomQuery
from flowmachine.core.spatial_unit import CellSpatialUnit
from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.features.subscriber.daily_location import daily_location
from flowmachine.features.utilities.subscriber_locations import BaseLocation
from flowmachine.features.subscriber.subscriber_stay_lengths import (
    SubscriberStayLengths,
)


class OneLocation(CustomQuery, BaseLocation):
    """
    Dummy subscriber location query that returns a single location for a single subscriber
    """

    def __init__(self, subscriber, location):
        self.spatial_unit = CellSpatialUnit()
        loc_string = f"'{location}'" if location is not None else "NULL"
        super().__init__(
            f"SELECT '{subscriber}' AS subscriber, {loc_string} AS location_id",
            column_names=["subscriber", "location_id"],
        )


@pytest.mark.parametrize(
    "statistic, expected",
    [
        ("count", 5),
        ("sum", 9),
        ("avg", 9 / 5),
        ("max", 3),
        ("min", 1),
        ("median", 2),
        ("stddev", np.sqrt(0.7)),
        ("variance", 0.7),
    ],
)
def test_subscriber_stay_lengths_statistics(statistic, expected, get_dataframe):
    """
    Test that SubscriberStayLengths returns the correct results in one example case
    """
    locations = [
        OneLocation("a_subscriber", locid)
        for locid in ["A", "A", "B", "C", "C", "C", "A", "D", "D"]
    ]
    ssl = SubscriberStayLengths(locations=locations, statistic=statistic)
    df = get_dataframe(ssl)
    assert len(df) == 1
    assert df.loc[0, "subscriber"] == "a_subscriber"
    assert df.loc[0, "value"] == expected


def test_subscriber_stay_lengths_splits_on_inactive(get_dataframe):
    """
    Test that consecutive stays do not continue across inactive periods
    """
    locations = [
        OneLocation("a_subscriber", "A"),
        OneLocation("a_subscriber", "A"),
        OneLocation("different_subscriber", "A"),
        OneLocation("a_subscriber", "A"),
    ]
    ssl = SubscriberStayLengths(locations=locations, statistic="max")
    df = get_dataframe(ssl).set_index("subscriber")
    assert df.loc["a_subscriber", "value"] == 2


@pytest.mark.parametrize("statistic", ["count", "max"])
def test_subscriber_stay_lengths_excludes_unlocatable(statistic, get_dataframe):
    """
    Test that NULL locations are not counted as stays
    """
    locations = [
        OneLocation("a_subscriber", "A"),
        OneLocation("a_subscriber", None),
        OneLocation("a_subscriber", None),
    ]
    ssl = SubscriberStayLengths(locations=locations, statistic=statistic)
    df = get_dataframe(ssl).set_index("subscriber")
    assert df.loc["a_subscriber", "value"] == 1


def test_subscriber_stay_lengths_result(get_dataframe, exemplar_spatial_unit_param):
    """
    Test that the result of a SubscriberStayLengths query has the expected columns and rows
    """
    locations = [
        daily_location(d, spatial_unit=exemplar_spatial_unit_param)
        for d in ["2016-01-01", "2016-01-02", "2016-01-03"]
    ]
    ssl = SubscriberStayLengths(locations=locations, statistic="max")
    df = get_dataframe(ssl)
    # Check that column_names property is accurate
    assert list(df.columns) == ssl.column_names
    # Check that 'subscriber' column is unique
    assert df.subscriber.is_unique
    # Check that all subscribers are included
    assert len(df) == 500


def test_subscriber_stay_lengths_mismatched_spatial_unit_raises():
    """
    Test that SubscriberStayLengths raises an error if locations
    have mismatched spatial units
    """
    with pytest.raises(InvalidSpatialUnitError):
        ssl = SubscriberStayLengths(
            locations=[
                daily_location(
                    "2016-01-01", spatial_unit=make_spatial_unit("admin", level=2)
                ),
                daily_location(
                    "2016-01-02", spatial_unit=make_spatial_unit("admin", level=3)
                ),
            ]
        )


def test_subscriber_stay_lengths_invalid_statistic_raises():
    with pytest.raises(ValueError, match="'invalid' is not a valid Statistic"):
        """
        Test that SubscriberStayLengths raises an error if the specified statistic
        is not one of valid_stats
        """
        ssl = SubscriberStayLengths(
            locations=[
                daily_location(
                    "2016-01-01", spatial_unit=make_spatial_unit("admin", level=3)
                ),
                daily_location(
                    "2016-01-02", spatial_unit=make_spatial_unit("admin", level=3)
                ),
            ],
            statistic="invalid",
        )
