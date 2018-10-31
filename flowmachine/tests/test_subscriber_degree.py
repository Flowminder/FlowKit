# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Test the subscriber degree class
"""

import pandas as pd
from unittest import TestCase

from flowmachine.features.subscriber.subscriber_degree import (
    SubscriberDegree,
    SubscriberInDegree,
    SubscriberOutDegree,
)


class test_SubscriberDegree(TestCase):
    """
    Tests for SubscriberDegree class
    """

    def setUp(self):
        self.ud = SubscriberDegree("2016-01-01", "2016-01-04")
        self.df = self.ud.get_dataframe()
        self.expected_columns = ["subscriber", "degree"]

    def test_returns_df(self):
        """
        SubscriberDegree().get_dataframe() returns a dataframe.
        """
        self.assertIs(type(self.df), pd.DataFrame)

    def test_returns_correct_column_names(self):
        """
        SubscriberDegree() dataframe has expected column names.
        """
        self.assertEquals(self.ud.column_names, self.expected_columns)

    def test_returns_correct_values(self):
        """
        SubscriberDegree() dataframe contains expected values.
        """
        # We expect subscriber '2Dq97XmPqvL6noGk' to have a single event in df1
        # and two events in df2 (due to the larger time interval).
        ud1 = SubscriberDegree(
            "2016-01-01 12:35:00", "2016-01-01 12:40:00", table="events.sms"
        )
        ud2 = SubscriberDegree(
            "2016-01-01 12:28:00", "2016-01-01 12:40:00", table="events.sms"
        )

        df1 = ud1.get_dataframe().set_index("subscriber")
        df2 = ud2.get_dataframe().set_index("subscriber")

        self.assertEquals(df1.ix["2Dq97XmPqvL6noGk"]["degree"], 1)
        self.assertEquals(df2.ix["2Dq97XmPqvL6noGk"]["degree"], 2)

    def test_returns_correct_in_out_values(self):
        """
        SubscriberIn/OutDegree() dataframe contains expected values.
        """
        # We expect subscriber '2Dq97XmPqvL6noGk' to not appear in df1, because they
        # only received a text, and to have degree 1 in in df2 because they
        # also sent one.
        ud1 = SubscriberInDegree(
            "2016-01-01 12:35:00", "2016-01-01 12:40:00", table="events.sms"
        )
        ud2 = SubscriberOutDegree(
            "2016-01-01 12:28:00", "2016-01-01 12:40:00", table="events.sms"
        )

        df1 = ud1.get_dataframe()
        df2 = ud2.get_dataframe().set_index("subscriber")

        self.assertNotIn("2Dq97XmPqvL6noGk", df1.subscriber.values)
        self.assertEquals(df2.ix["2Dq97XmPqvL6noGk"]["degree"], 1)
