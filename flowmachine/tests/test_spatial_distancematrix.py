# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the DistanceMatrix() class.
"""


from flowmachine.features.spatial import DistanceMatrix
from flowmachine.core.spatial_unit import VersionedSiteSpatialUnit


def test_some_results(get_dataframe):
    """
    DistanceMatrix() returns a dataframe that contains hand-picked results.
    """
    c = DistanceMatrix(spatial_unit=VersionedSiteSpatialUnit())
    df = get_dataframe(c)
    set_df = df.set_index("site_id_from")
    assert round(set_df.loc["8wPojr"]["distance"].values[0]) == 789
    assert round(set_df.loc["8wPojr"]["distance"].values[2]) == 769
    assert round(set_df.loc["8wPojr"]["distance"].values[4]) == 758


def test_result_has_correct_length(get_length):
    """
    DistanceMatrix() has the correct length.
    """
    c = DistanceMatrix(spatial_unit=VersionedSiteSpatialUnit())
    assert get_length(c) == 35 ** 2
