# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the DistanceMatrix() class.
"""

import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.core.errors import UnstorableQueryError
from flowmachine.features.spatial import DistanceMatrix


def test_some_results(get_dataframe):
    """
    DistanceMatrix() returns a dataframe that contains hand-picked results.
    """
    c = DistanceMatrix(spatial_unit=make_spatial_unit("versioned-site"))
    df = get_dataframe(c)
    set_df = df.set_index(["site_id_from", "version_from", "site_id_to", "version_to"])
    assert set_df.loc[("8wPojr", 1, "GN2k0G", 0)]["value"] == pytest.approx(
        789.23239740488
    )
    assert set_df.loc[("8wPojr", 0, "GN2k0G", 0)]["value"] == pytest.approx(
        769.20155628077
    )
    assert set_df.loc[("8wPojr", 1, "DbWg4K", 0)]["value"] == pytest.approx(
        757.97771793683
    )


@pytest.mark.parametrize(
    "spatial_unit_type, length",
    [("versioned-cell", 3844), ("versioned-site", 1225), ("lon-lat", 1089)],
)
def test_result_has_correct_length(spatial_unit_type, length, get_length):
    """
    DistanceMatrix has the correct length.
    """
    c = DistanceMatrix(spatial_unit=make_spatial_unit(spatial_unit_type))
    assert get_length(c) == length


def test_not_storeable():
    with pytest.raises(UnstorableQueryError):
        DistanceMatrix(spatial_unit=make_spatial_unit("versioned-site")).store()
