# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.core import make_spatial_unit
from flowmachine.features import UniqueLocationCounts, SubscriberLocations


def test_returns_errors():
    """
    Test spatial unit exists
    """
    with pytest.raises(InvalidSpatialUnitError):
        UniqueLocationCounts("2016-01-01", "2016-01-02", spatial_unit="foo")


def test_column_names_unique_location_counts(exemplar_spatial_unit_param):
    """Test that column_names property matches head(0) for UniqueLocationCounts"""
    lv = UniqueLocationCounts(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=exemplar_spatial_unit_param,
        hours=(5, 17),
    )
    assert lv.head(0).columns.tolist() == lv.column_names


def test_correct_counts(get_dataframe):
    """
    UniqueLocationCounts returns correct counts.
    """
    ulc = UniqueLocationCounts(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=make_spatial_unit("cell"),
        hours=(5, 17),
    )
    df = get_dataframe(ulc)
    dful = get_dataframe(
        SubscriberLocations(
            "2016-01-01",
            "2016-01-02",
            spatial_unit=make_spatial_unit("cell"),
            hours=(5, 17),
        )
    )
    assert [df["value"][0], df["value"][1], df["value"][2]] == [
        len(dful[dful["subscriber"] == df["subscriber"][0]]["location_id"].unique()),
        len(dful[dful["subscriber"] == df["subscriber"][1]]["location_id"].unique()),
        len(dful[dful["subscriber"] == df["subscriber"][2]]["location_id"].unique()),
    ]
