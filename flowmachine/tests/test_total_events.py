# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Test the total events class
"""

import pandas as pd
from unittest import TestCase

from flowmachine.features.subscriber import TotalSubscriberEvents


class test_TotalSubscriberEvents(TestCase):
    """
    Tests for TotalSubscriberEvents class
    """

    def setUp(self):
        self.te = TotalSubscriberEvents("2016-01-01", "2016-01-04")
        self.df = self.te.get_dataframe()

    def test_returns_df(self):
        """
        TotalSubscriberEvents().get_dataframe() returns a dataframe.
        """

        self.assertIs(type(self.df), pd.DataFrame)

    def test_some_results(self):
        """
        TotalSubscriberEvents() returns a dataframe that contains hand-picked results.
        """

        set_df = self.df.set_index("subscriber")
        self.assertEqual(set_df.ix["1d29oEA95KEzAKlW"][0], 16)
        self.assertEqual(set_df.ix["x8o7D2Ax8Lg5Y0MB"][0], 18)
        self.assertEqual(set_df.ix["yObw75JkAZ0vKRlz"][0], 5)

    def test_subset_by_calls(self):
        """
        TotalSubscriberEvents() get only those events which are calls.
        """
        df = (
            TotalSubscriberEvents("2016-01-01", "2016-01-04", event_type="calls")
            .get_dataframe()
            .set_index("subscriber")
        )
        self.assertEqual(df.ix["038OVABN11Ak4W5P"][0], 9)

    def test_subset_by_outgoing(self):
        """
        TotalSubscriberEvents() subsets those calls that are outgoing only.
        """
        df = (
            TotalSubscriberEvents("2016-01-01", "2016-01-04", direction="out")
            .get_dataframe()
            .set_index("subscriber")
        )
        self.assertEqual(df.ix["038OVABN11Ak4W5P"][0], 4)

    def test_double_subset(self):
        """
        TotalSubscriberEvents() gets only incoming texts.
        """
        df = (
            TotalSubscriberEvents(
                "2016-01-01", "2016-01-04", event_type="sms", direction="in"
            )
            .get_dataframe()
            .set_index("subscriber")
        )
        self.assertEqual(df.ix["038OVABN11Ak4W5P"][0], 3)
