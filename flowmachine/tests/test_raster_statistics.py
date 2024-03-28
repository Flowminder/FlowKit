# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the RasterStatistics utilities.
"""

import pytest

from flowmachine.core import Table
from flowmachine.features.raster import RasterStatistics


def test_computes_expected_clipping_values(get_dataframe):
    """
    RasterStatistics() returns correct values when clipping vector and raster layers.
    """
    G = "admin2pcod"
    vector = Table(schema="geography", name="admin2", columns=["admin2pcod", "geom"])
    r = RasterStatistics(
        raster="population.small_nepal_raster", vector=vector, grouping_element=G
    )

    result = get_dataframe(r)  # Should have only _one_ entry
    assert 1 == len(result)
    assert "524 3 07" in result.admin2pcod.values
    assert 2500000.0 == result.set_index("admin2pcod").loc["524 3 07"][0]


def test_computes_expected_total_value(get_dataframe):
    """
    RasterStatistics() computes correct total value of raster.
    """
    r = RasterStatistics(raster="population.small_nepal_raster")
    result = get_dataframe(r)

    assert result["statistic"].iloc[0] == 2500000


def test_raises_notimplemented_when_wrong_statistic_requested():
    """
    RasterStatistics() raises NotImplementedError if wrong statistic requested.
    """
    G = "admin2pcod"
    vector = Table(schema="geography", name="admin2", columns=["admin2pcod", "geom"])
    with pytest.raises(NotImplementedError):
        r = RasterStatistics(
            raster="population.small_nepal_raster",
            vector=vector,
            grouping_element=G,
            statistic="foobar",
        )


def test_raises_valueerror_when_grouping_element_not_provided():
    """
    RasterStatistics() raises ValueError when `grouping_element` not provided.
    """
    G = None
    vector = Table(schema="geography", name="admin2", columns=["admin2pcod", "geom"])
    with pytest.raises(ValueError):
        r = RasterStatistics(
            "population.small_nepal_raster", vector=vector, grouping_element=None
        )


def test_raster_statistics_column_names_novector(get_dataframe):
    """
    Test that column_names property matches head(0) for RasterStatistics
    when vector is None
    """
    r = RasterStatistics(raster="population.small_nepal_raster")
    assert get_dataframe(r).columns.tolist() == r.column_names


def test_raster_statistics_column_names_vector(get_dataframe):
    """
    Test that column_names property matches head(0) for RasterStatistics
    when vector is not None
    """
    vector = Table(schema="geography", name="admin2", columns=["admin2pcod", "geom"])
    r = RasterStatistics(
        raster="population.small_nepal_raster",
        vector=vector,
        grouping_element="admin2pcod",
    )
    assert get_dataframe(r).columns.tolist() == r.column_names
