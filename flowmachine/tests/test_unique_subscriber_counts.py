# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the UniqueSubscriberCounts query 
"""

import pytest
from flowmachine.features import UniqueSubscriberCounts
from flowmachine.features.utilities import subscriber_locations
from flowmachine.core.errors import BadLevelError


@pytest.mark.usefixtures("skip_datecheck")
def test_unique_subscriber_counts_column_names(exemplar_level_param):
    """
    Test that column_names property of UniqueSubscriberCounts matches head(0)
    """
    usc = UniqueSubscriberCounts("2016-01-01", "2016-01-04", **exemplar_level_param)
    assert usc.head(0).columns.tolist() == usc.column_names


def test_returns_errors():
    """
    Test level exists
    """
    with pytest.raises(BadLevelError):
        UniqueSubscriberCounts("2016-01-01", "2016-01-02", level="BAD_LEVEL")


def test_correct_counts(get_dataframe):
    """
    UniqueLocationCounts returns correct counts.
    """
    usc = UniqueSubscriberCounts(
        "2016-01-01", "2016-01-02", level="cell", hours=(5, 17)
    )
    df = get_dataframe(usc)
    dful = get_dataframe(
        subscriber_locations(
            "2016-01-01", "2016-01-02", spatial_unit=None, hours=(5, 17)
        )
    )
    assert [
        df["unique_subscriber_counts"][0],
        df["unique_subscriber_counts"][1],
        df["unique_subscriber_counts"][2],
    ] == [
        len(dful[dful["location_id"] == df["location_id"][0]]["subscriber"].unique()),
        len(dful[dful["location_id"] == df["location_id"][1]]["subscriber"].unique()),
        len(dful[dful["location_id"] == df["location_id"][2]]["subscriber"].unique()),
    ]
