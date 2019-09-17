# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the DistanceMatrix() class.
"""

import pytest

from flowmachine.features.spatial import DistanceMatrix
from flowmachine.core import make_spatial_unit


def test_some_results(get_dataframe):
    """
    DistanceMatrix() returns a dataframe that contains hand-picked results.
    """
    c = DistanceMatrix(spatial_unit=make_spatial_unit("versioned-site"))
    df = get_dataframe(c)
    set_df = df.set_index("site_id_from")
    assert set_df.loc["8wPojr"]["distance"].values[0] == pytest.approx(789)
    assert set_df.loc["8wPojr"]["distance"].values[2] == pytest.approx(769)
    assert set_df.loc["8wPojr"]["distance"].values[4] == pytest.approx(758)


@pytest.mark.parametrize(
    "spatial_unit_type, length",
    [("versioned-cell", 3844), ("versioned-site", 1225), ("lon-lat", 2638)],
)
def test_result_has_correct_length(spatial_unit_type, length, get_length):
    """
    DistanceMatrix has the correct length.
    """
    c = DistanceMatrix(spatial_unit=make_spatial_unit(spatial_unit_type))
    assert get_length(c) == length
