# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the VersionedInfrastructure() class.
"""

import pandas as pd
from unittest import TestCase

from flowmachine.features import VersionedInfrastructure


class VersionedInfrastructureTestCase(TestCase):
    """
    Tests for the VersionedInfrastructure() class
    """

    def setUp(self):
        """
        Method for instantiating test environment.
        """
        self.n_sites = 30

    def test_returns_correct_results(self):
        """
        VersionedInfrastructure() returns N-sized result set.
        """
        result = VersionedInfrastructure()
        self.assertIs(len(result), self.n_sites)

    def test_raises_error_if_wrong_table_used(self):
        """
        VersionedInfrastructure() raises error if not using 'sites' or 'cells' tables.
        """
        with self.assertRaises(ValueError):
            VersionedInfrastructure(table="xxx")
