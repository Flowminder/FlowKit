# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.errors import BadLevelError
from flowmachine.features import UniqueLocationCounts, subscriber_locations


def test_returns_errors():
    """
    Test level exists
    """
    with pytest.raises(BadLevelError):
        UniqueLocationCounts("2016-01-01", "2016-01-02", level="foo")


def test_column_names_unique_location_counts(exemplar_level_param):
    """ Test that column_names property matches head(0) for UniqueLocationCounts"""
    lv = UniqueLocationCounts(
        "2016-01-01", "2016-01-02", **exemplar_level_param, hours=(5, 17)
    )
    assert lv.head(0).columns.tolist() == lv.column_names


def test_correct_counts(get_dataframe):
    """
    UniqueLocationCounts returns correct counts.
    """
    ulc = UniqueLocationCounts("2016-01-01", "2016-01-02", level="cell", hours=(5, 17))
    df = get_dataframe(ulc)
    dful = get_dataframe(
        subscriber_locations("2016-01-01", "2016-01-02", level="cell", hours=(5, 17))
    )
    assert [
        df["unique_location_counts"][0],
        df["unique_location_counts"][1],
        df["unique_location_counts"][2],
    ] == [
        len(dful[dful["subscriber"] == df["subscriber"][0]]["location_id"].unique()),
        len(dful[dful["subscriber"] == df["subscriber"][1]]["location_id"].unique()),
        len(dful[dful["subscriber"] == df["subscriber"][2]]["location_id"].unique()),
    ]
