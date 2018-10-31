# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the class FirstLocation
"""

from unittest import TestCase
import pandas as pd

from flowmachine.features.subscriber import FirstLocation


class test_FirstLocation(TestCase):
    def setUp(self):
        self.dfl = FirstLocation(
            "2016-01-01", "2016-01-04", location="QeBRM8", level="versioned-site"
        )
        self.df = self.dfl.get_dataframe()

    def test_returns_df(self):
        """
        FirstLocation().get_dataframe() returns a dataframe.
        """

        self.assertIs(type(self.df), pd.DataFrame)

    def test_some_results(self):
        """
        FirstLocation() dataframe contains hand-picked records.
        """

        set_df = self.df.set_index("subscriber")
        self.assertEqual(
            str(set_df.ix["038OVABN11Ak4W5P"]), "2016-01-01 05:02:10+00:00"
        )
        self.assertEqual(
            str(set_df.ix["1p4MYbA1Y4bZzBQa"]), "2016-01-02 21:30:41+00:00"
        )
        self.assertEqual(
            str(set_df.ix["3XKdxqvyNxO2vLD1"]), "2016-01-01 05:09:20+00:00"
        )

    def test_handles_list_of_locations(self):
        """
        FirstLocation() subsets data based on a list of locations, rather than a single one.
        """
        dfl = FirstLocation(
            "2016-01-01",
            "2016-01-04",
            location=["QeBRM8", "m9jL23" "LVnDQL"],
            level="versioned-site",
        )
        df = dfl.get_dataframe()

        df.set_index("subscriber", inplace=True)
        self.assertEqual(str(df.ix["038OVABN11Ak4W5P"]), "2016-01-01 05:02:10+00:00")

    def test_can_be_called_with_any(self):
        """
        FirstLocation() can call first at location with the keyword 'any'.
        """
        dfl = FirstLocation("2016-01-03", "2016-01-04", location="any")
        df = dfl.get_dataframe()
        df.set_index("subscriber", inplace=True)
        self.assertEqual(str(df.ix["0MQ4RYeKn7lryxGa"]), "2016-01-03 01:38:56+00:00")
