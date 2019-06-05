# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the DistanceMatrix() class.
"""

import pytest

from flowmachine.features.spatial import DistanceMatrix
from flowmachine.core.spatial_unit import (
    versioned_cell_spatial_unit,
    versioned_site_spatial_unit,
    lat_lon_spatial_unit,
)


def test_some_results(get_dataframe):
    """
    DistanceMatrix() returns a dataframe that contains hand-picked results.
    """
    c = DistanceMatrix(spatial_unit=versioned_site_spatial_unit())
    df = get_dataframe(c)
    set_df = df.set_index("site_id_from")
    assert round(set_df.loc["8wPojr"]["distance"].values[0]) == 789
    assert round(set_df.loc["8wPojr"]["distance"].values[2]) == 769
    assert round(set_df.loc["8wPojr"]["distance"].values[4]) == 758


@pytest.mark.parametrize(
    "spatial_unit_type, length",
    [
        (versioned_cell_spatial_unit, 62),
        (versioned_site_spatial_unit, 35),
        (lat_lon_spatial_unit, 62),
    ],
)
def test_result_has_correct_length(spatial_unit_type, length, get_length):
    """
    DistanceMatrix() has the correct length.
    """
    c = DistanceMatrix(spatial_unit=spatial_unit_type())
    assert get_length(c) == length ** 2
