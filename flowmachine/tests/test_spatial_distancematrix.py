# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the DistanceMatrix() class.
"""

import pandas as pd
from unittest import TestCase

from flowmachine.features.spatial import DistanceMatrix


class DistanceMatrixTestCase(TestCase):
    """
    Tests for the DistanceMatrix() class.

    """

    def setUp(self):
        """
        Method for instantiating test environment.
        """
        self.n_sites = 35
        self.c = DistanceMatrix(level="versioned-site")
        self.df = self.c.get_dataframe()

    def test_some_results(self):
        """
        DistanceMatrix() returns a dataframe that contains hand-picked results.
        """
        set_df = self.df.set_index("site_id_from")
        self.assertAlmostEqual(
            round(set_df.loc["8wPojr"]["distance"].values[0]), 789, 2
        )
        self.assertAlmostEqual(
            round(set_df.loc["8wPojr"]["distance"].values[2]), 769, 2
        )
        self.assertAlmostEqual(
            round(set_df.loc["8wPojr"]["distance"].values[4]), 758, 2
        )

    def test_result_has_correct_length(self):
        """
        DistanceMatrix() has the correct length.
        """
        self.assertEqual(len(self.df), self.n_sites ** 2)
