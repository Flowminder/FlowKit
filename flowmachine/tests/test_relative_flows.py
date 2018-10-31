# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest import TestCase
from flowmachine.features.subscriber import *
from flowmachine.features import daily_location, Flows
import pandas as pd


class TestRelativeFlow(TestCase):
    def setUp(self):
        self.dl1 = daily_location("2016-01-01")
        self.dl2 = daily_location("2016-01-02")
        self.dl3 = daily_location("2016-01-07")
        self.flowA = Flows(self.dl1, self.dl2)
        self.flowB = Flows(self.dl1, self.dl3)
        self.relfl = self.flowA - self.flowB
        self.df_rel = self.relfl.get_dataframe()

    def test_flows_raise_error(self):
        """
        Flows() raises error if location levels are different.
        """
        dl1 = daily_location("2016-01-01", level="admin3")
        dl2 = daily_location("2016-01-01", level="admin2")
        with self.assertRaises(ValueError):
            Flows(dl1, dl2)

    def test_a_sub_b(self):
        """
        Flows() between two locations returns expected positive value.
        """
        diff = self.df_rel[
            (self.df_rel.name_from == "Bajhang") & (self.df_rel.name_to == "Dadeldhura")
        ]["count"].values[0]
        self.assertEqual(1, diff)

    def test_a_sub_b_negative(self):
        """
        Flows() between two locations returns expected negattive value.
        """
        # Bajhang    Myagdi  3   4.0
        diff = self.df_rel[
            (self.df_rel.name_from == "Bajhang") & (self.df_rel.name_to == "Myagdi")
        ]["count"].values[0]
        self.assertEqual(-1, diff)

    def test_sub_commutative(self):
        """
        Flows() subtraction is a noncommutative operation.
        """
        diff = (self.flowA - (self.flowA - self.flowB)).get_dataframe()
        diff = diff[(diff.name_from == "Bajhang") & (diff.name_to == "Myagdi")][
            "count"
        ].values[0]
        self.assertEqual(4, diff)

    def test_sub_scalar(self):
        """
        Flows() subtraction of a scalar gives a known value.
        """
        diff = (self.flowA - 3).get_dataframe()
        diff = diff[(diff.name_from == "Bajhang") & (diff.name_to == "Myagdi")][
            "count"
        ].values[0]
        self.assertEqual(0, diff)

    def test_a_sub_b_no_b_flow(self):
        """
        Flows() between two locations where one location is NA should return the flow of the non NA location.
        """
        # Humla  Kapilbastu  2   NaN
        diff = self.df_rel[
            (self.df_rel.name_from == "Humla") & (self.df_rel.name_to == "Kapilbastu")
        ]["count"].values[0]
        self.assertEqual(2, diff)

    def test_abs_diff_equal(self):
        """
        The absolute difference between flows A and B should be equal for A - B and B - A.
        """
        relfl_reverse = (self.flowB - self.flowA).get_dataframe()
        compare = abs(
            relfl_reverse.set_index(["name_from", "name_to"]).sort_index()
        ) == abs(self.df_rel.set_index(["name_from", "name_to"]).sort_index())
        self.assertTrue(compare.all().values[0])
