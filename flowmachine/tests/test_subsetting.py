# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests the check the queries subset method.
"""
import pandas as pd

from unittest import TestCase
from flowmachine.features import daily_location, RadiusOfGyration
from flowmachine.core import CustomQuery, Table
from numpy.testing import assert_array_equal


class TestNumericSubsetting(TestCase):
    def setUp(self):
        self.rog = RadiusOfGyration("2016-01-01", "2016-01-02")
        self.low = 150
        self.high = 155
        self.rog_df = self.rog.get_dataframe().query(
            "{low} <= rog <= {high}".format(low=self.low, high=self.high)
        )
        self.sub = self.rog.numeric_subset(col="rog", low=self.low, high=self.high)

    def _query_has_values(self, Q, expected_df):
        """
        Test if the values of a dataframes columns are equal
        to certain values.
        """
        query_df = Q.get_dataframe()
        assert_array_equal(expected_df.values, query_df.values)

    def test_can_numsubset_with_low_and_high(self):
        """
        flowmachine.RadiusOfGyration can be subset within a range
        """

        self._query_has_values(self.sub, self.rog_df)

    def test_can_numsubset_with_inf(self):
        """
        flowmachine.RadiusOfGyration can be subset between -Inf and Inf
        """

        low = -float("Infinity")
        high = float("Infinity")
        sub = self.rog.numeric_subset(col="rog", low=low, high=high)
        df = self.rog.get_dataframe().query(
            "{low} <= rog <= {high}".format(low=low, high=high)
        )
        self._query_has_values(sub, df)

    def test_call_with_str_raises_error(self):
        """
        Numeric subset can't be called with a string in arguments low and high
        """
        with self.assertRaises(TypeError):
            self.rog.numeric_subset(col="rog", low="foo", high=self.high)
        with self.assertRaises(TypeError):
            self.rog.numeric_subset(col="rog", low=self.low, high="bar")

    def test_can_be_stored(self):
        """
        Test that flowmachine.NumericSubset can be stored.
        """
        self.sub.store().result()
        self.assertTrue(self.sub.is_stored)
        # Test that the store is of the right length
        sub = self.rog.numeric_subset(col="rog", low=self.low, high=self.high)
        self.assertEqual(len(sub.get_dataframe()), len(self.rog_df))

    def tearDown(self):
        """
        Remove stored table from "can_be_stored" test.
        """
        self.sub.invalidate_db_cache()


class TestSubsetting(TestCase):
    def setUp(self):

        self.dl = daily_location("2016-01-03")
        self.rog = RadiusOfGyration("2016-01-01", "2016-01-03")
        self.subscriber_list = list(self.dl.head(8).subscriber)
        self.single_subscriber = self.subscriber_list[3]

    def _query_has_values(self, Q, expected_values, column="subscriber"):
        """
        Test if the values of a dataframes columns are equal
        to certain values.
        """
        value_set = set(Q.get_dataframe()[column])
        self.assertEqual(set(expected_values), value_set)

    def test_can_subset_with_list_of_subscribers(self):
        """
        flowmachine.daily_location can be subset with a list of subscribers.
        """

        sub = self.dl.subset(col="subscriber", subset=self.subscriber_list)
        self._query_has_values(sub, self.subscriber_list)

    def test_can_subset_with_single_subscriber(self):
        """
        flowmachine.daily_location can be subset with a single subscriber.
        """

        sub = self.dl.subset(col="subscriber", subset=self.single_subscriber)
        self._query_has_values(sub, [self.single_subscriber])

    def test_can_subset_with_list_containing_single_subscriber(self):
        """
        flowmachine.daily_location can be subset with a list containing a single subscriber.
        """

        sub = self.dl.subset(col="subscriber", subset=[self.single_subscriber])
        self._query_has_values(sub, [self.single_subscriber])

    def test_can_getitem_location(self):
        """
        flowmachine.daily_location can use get_item
        """

        sub = self.dl[self.single_subscriber]
        self._query_has_values(sub, [self.single_subscriber])

    def test_special_chars(self):
        """Special characters don't break subsets"""
        sub = self.dl.subset(col="subscriber", subset=["Murray'"])
        sub.get_dataframe()
        sub = self.dl.subset(col="subscriber", subset=["Murray'", "Horace"])
        sub.get_dataframe()
        sub = self.dl.subset(col="subscriber", subset="Murray'")
        sub.get_dataframe()
        sub = self.dl.subset(col="subscriber", subset="Murray'")
        sub.get_dataframe()

    def test_can_get_item_subscriber_metric(self):
        """
        flowmachine.SubscriberFeature allows for getting items
        """

        sub = self.rog[self.single_subscriber]
        self._query_has_values(sub, [self.single_subscriber])

    def test_calling_parent_does_break_subsetting(self):
        """
        flowmachine.Subset doesn't inherit dataframe from parent
        """

        # In the past I have run into this error, so I'll test for
        # it explicitly. If the thing that gets subset has already
        # had _df stored, then if we naively inherited everything
        # then we would get the parent dataframe when asking for it
        dl = daily_location("2016-01-03")
        dl.get_dataframe()
        dl_sub = dl[self.subscriber_list]
        self.assertEqual(len(dl_sub), len(self.subscriber_list))
        self.assertEqual(len(dl_sub.get_dataframe()), len(self.subscriber_list))

    def test_can_be_stored(self):
        """
        Test that flowmachine.Subset can be stored.
        """

        sub = self.dl.subset(col="subscriber", subset=self.subscriber_list)
        sub.store().result()
        self.assertTrue(sub.is_stored)
        # Test that the store is of the right length
        sub = self.dl.subset(col="subscriber", subset=self.subscriber_list)
        self.assertEqual(len(sub), len(self.subscriber_list))
        sub.invalidate_db_cache()


class TestSubsetOfSubset(TestCase):
    def setUp(self):

        self.t = Table("geography.admin3")
        self.t_df = self.t.get_dataframe()

    def test_subset_subset(self):

        """
        This test applies two non-numeric subsets one
        after the other .
        """

        sub_cola = "admin1name"
        sub_vala = "Central Development Region"
        sub_colb = "admin2name"
        sub_valb = "Bagmati"

        sub_q = self.t.subset(sub_cola, sub_vala).subset(sub_colb, sub_valb)
        sub_df = self.t_df[
            (self.t_df[sub_cola] == sub_vala) & (self.t_df[sub_colb] == sub_valb)
        ]
        sub_df = sub_df.reset_index(drop=True)

        self.assertTrue(sub_q.get_dataframe().equals(sub_df))

    def test_subset_subsetnumeric(self):
        """
        This test applies a non-numeric subsets and 
        a numeric subset one after another in both possible
        orders.
        """

        sub_cola = "admin1name"
        sub_vala = "Central Development Region"
        sub_colb = "shape_area"
        sub_lowb = 0.1
        sub_highb = 0.12

        sub_q1 = self.t.subset(sub_cola, sub_vala).numeric_subset(
            sub_colb, sub_lowb, sub_highb
        )
        sub_q2 = self.t.numeric_subset(sub_colb, sub_lowb, sub_highb).subset(
            sub_cola, sub_vala
        )
        sub_df = self.t_df[
            (self.t_df[sub_cola] == sub_vala)
            & (sub_lowb <= self.t_df[sub_colb])
            & (self.t_df[sub_colb] <= sub_highb)
        ]
        sub_df = sub_df.reset_index(drop=True)

        self.assertTrue(sub_q1.get_dataframe().equals(sub_df))
        self.assertTrue(sub_q2.get_dataframe().equals(sub_df))

    def test_subsetnumeric_subsetnumeric(self):

        """
        This test applies two numeric subsets one
        after the other 
        """

        sub_cola = "shape_star"
        sub_lowa = 0.1
        sub_higha = 0.2
        sub_colb = "shape_leng"
        sub_lowb = 1.0
        sub_highb = 2.0

        sub_q = self.t.numeric_subset(sub_cola, sub_lowa, sub_lowb).numeric_subset(
            sub_colb, sub_lowb, sub_highb
        )
        sub_df = self.t_df[
            (sub_lowa <= self.t_df[sub_cola])
            & (self.t_df[sub_cola] <= sub_higha)
            & (sub_lowb <= self.t_df[sub_colb])
            & (self.t_df[sub_colb] <= sub_highb)
        ]
        sub_df = sub_df.reset_index(drop=True)

        self.assertTrue(sub_q.get_dataframe().equals(sub_df))
