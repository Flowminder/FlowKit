# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the feature subscriber event proportions.
"""

import pandas as pd
from unittest import TestCase

from flowmachine.features.subscriber.proportion_outgoing import ProportionOutgoing


class ProportionOutgoingTestCase(TestCase):
    """
    Tests for ProportionOutgoings() feature class.
    """

    def setUp(self):
        self.ud = ProportionOutgoing("2016-01-01", "2016-01-04")
        self.df = self.ud.get_dataframe()
        self.expected_columns = [
            "subscriber",
            "proportion_outgoing",
            "proportion_incoming",
        ]

    def test_returns_df(self):
        """
        ProportionOutgoing().get_dataframe() returns a dataframe.
        """
        self.assertIs(type(self.df), pd.DataFrame)

    def test_returns_correct_column_names(self):
        """
        ProportionOutgoing() dataframe has expected column names.
        """
        self.assertEquals(self.ud.column_names, self.expected_columns)

    def test_returns_correct_values(self):
        """
        ProportionOutgoing() dataframe contains expected values.
        """
        df1 = self.ud.get_dataframe().set_index("subscriber")
        self.assertEquals(df1.ix["ZM3zYAPqx95Rw15J"]["proportion_outgoing"], 0.600000)
        self.assertEquals(df1.ix["ZM3zYAPqx95Rw15J"]["proportion_incoming"], 0.400000)

    def test_passing_not_known_subscriber_identifier_raises_error(self):
        """
        ProportionOutgoing() passing not know `subscriber_identifier` raises error.
        """
        with self.assertRaises(ValueError):
            ProportionOutgoing("2016-01-01", "2016-01-04", subscriber_identifier="foo")
