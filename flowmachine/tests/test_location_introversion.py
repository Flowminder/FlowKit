# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the LocationIntroversion() class.
"""


import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features.location import LocationIntroversion


@pytest.mark.usefixtures("skip_datecheck")
def test_location_introversion_column_names(exemplar_spatial_unit_param):
    """Test that column_names property matches head(0)"""
    li = LocationIntroversion(
        "2016-01-01", "2016-01-07", spatial_unit=exemplar_spatial_unit_param
    )
    assert li.head(0).columns.tolist() == li.column_names


def test_some_results(get_dataframe):
    """
    LocationIntroversion() returns a dataframe that contains hand-picked results.
    """
    df = get_dataframe(
        LocationIntroversion(
            "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("admin", level=3)
        )
    )
    set_df = df.set_index("pcod")
    assert round(set_df.loc["524 4 12 62"]["value"], 6) == pytest.approx(0.108517)
    assert round(set_df.loc["524 3 08 44"]["value"], 6) == pytest.approx(0.097884)
    assert round(1 - set_df.loc["524 2 04 21"]["value"], 6) == pytest.approx(0.936842)


def test_lon_lat_introversion(get_dataframe):
    df = get_dataframe(
        LocationIntroversion(
            "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("lon-lat")
        )
    )
    assert pytest.approx(0.0681818181818182) == df.value.max()
    assert (
        pytest.approx([83.7762949093138, 28.2715052907426])
        == df.sort_values("value").iloc[0][["lon", "lat"]].tolist()
    )


def test_no_result_is_greater_than_one(get_dataframe):
    """
    No results from LocationIntroversion()['introversion'] is greater than 1.
    """
    df = get_dataframe(
        LocationIntroversion(
            "2016-01-01", "2016-01-07", spatial_unit=make_spatial_unit("admin", level=3)
        )
    )
    results = df[df["value"] > 1]
    assert len(results) == 0
