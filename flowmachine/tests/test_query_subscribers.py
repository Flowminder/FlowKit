# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the subscriber query stuff
"""

import pytz
import warnings
import pandas as pd
import psycopg2 as pg

from datetime import datetime
from unittest import TestCase

from flowmachine.features import RadiusOfGyration, SubscriberDegree

from flowmachine.core import Table, Query
from flowmachine.core.errors import BadLevelError, MissingDateError

from flowmachine.features.utilities.sets import (
    EventTableSubset,
    UniqueSubscribers,
    SubscriberLocationSubset,
)

from flowmachine.features.subscriber.home_location import HomeLocation
from flowmachine.features.subscriber.last_location import LastLocation
from flowmachine.features.subscriber.daily_location import (
    daily_location,
    locate_subscribers,
)
from flowmachine.features.subscriber.most_frequent_location import MostFrequentLocation
from flowmachine.utils import list_of_dates


class test_subset_dates(TestCase):
    """
    Tests for the EventTableSubset class
    """

    def setUp(self):
        self.expected_columns = sorted(
            [
                "id",
                "outgoing",
                "datetime",
                "duration",
                "network",
                "subscriber",
                "msisdn_counterpart",
                "location_id",
                "imsi",
                "imei",
                "tac",
                "operator_code",
                "country_code",
            ]
        )

    def test_error_on_start_is_stop(self):
        """Test that a value error is raised when start == stop"""
        with self.assertRaises(ValueError):
            EventTableSubset("2016-01-01", "2016-01-01")

    def test_handles_dates(self):
        """
        Date subsetter can handle timestamp without hours or mins.
        """
        sd = EventTableSubset("2016-01-01", "2016-01-02")
        df = sd.get_dataframe()

        minimum = df["datetime"].min().to_pydatetime()
        maximum = df["datetime"].max().to_pydatetime()

        min_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 1))
        max_comparison = pytz.timezone("Etc/UTC").localize(datetime(2016, 1, 2))

        self.assertTrue(minimum.timestamp() > min_comparison.timestamp())
        self.assertTrue(maximum.timestamp() < max_comparison.timestamp())

    def test_warns_on_missing(self):
        """
        Date subsetter should warn on missing dates.
        """
        message = "115 of 122 calendar dates missing. Earliest date is 2016-01-01, latest is 2016-01-07"
        with self.assertWarnsRegex(UserWarning, message):
            EventTableSubset("2016-01-01", "2016-05-02")

    def test_error_on_all_missing(self):
        """
        Date subsetter should error when all dates are missing.
        """
        with self.assertRaises(MissingDateError):
            sd = EventTableSubset("2016-05-01", "2016-05-02")
        with self.assertRaises(MissingDateError):
            sd = EventTableSubset("2016-05-01", "2016-05-02", table="events.topups")

    def test_handles_mins(self):
        """
        Date subsetter can handle timestamps including the times.
        """
        sd = EventTableSubset("2016-01-01 13:30:30", "2016-01-02 16:25:00")
        df = sd.get_dataframe()

        minimum = df["datetime"].min().to_pydatetime()
        maximum = df["datetime"].max().to_pydatetime()

        min_comparison = pytz.timezone("Etc/UTC").localize(
            datetime(2016, 1, 1, 13, 30, 30)
        )
        max_comparison = pytz.timezone("Etc/UTC").localize(
            datetime(2016, 1, 2, 16, 25, 0)
        )

        self.assertTrue(minimum.timestamp() > min_comparison.timestamp())
        self.assertTrue(maximum.timestamp() < max_comparison.timestamp())

    def test_head_has_column_names(self):
        """
        Returning the head of the dataframe gives the expected column names.
        """
        sd = EventTableSubset("2016-01-01", "2016-01-02")
        head = sd.head()
        self.assertEqual(sorted(head.columns), self.expected_columns)

    def test_dataframe_has_column_names(self):
        """
        Returning the dataframe gives the expected column names.
        """
        sd = EventTableSubset("2016-01-01", "2016-01-02")
        df = sd.get_dataframe()
        self.assertEqual(sorted(df.columns), self.expected_columns)

    def test_can_subset_by_hour(self):
        """
        EventTableSubset() can subset by a range of hours
        """
        sd = EventTableSubset("2016-01-01", "2016-01-04", hours=(12, 17))
        df = sd.get_dataframe()
        df["hour"] = df.datetime.apply(lambda x: x.hour)
        df["day"] = df.datetime.apply(lambda x: x.day)
        Range = df.hour.max() - df.hour.min()
        self.assertEqual(Range, 4)
        # Also check that all the dates are still there
        self.assertTrue(3 in df.day and 2 in df.day and 1 in df.day)

    def test_handles_backwards_dates(self):
        """
        If the subscriber passes dates that are 'backwards' this will be interpreted as spanning midnight.
        """
        sd = EventTableSubset("2016-01-01", "2016-01-04", hours=(20, 5))
        df = sd.get_dataframe()
        df["hour"] = df.datetime.apply(lambda x: x.hour)
        df["day"] = df.datetime.apply(lambda x: x.day)
        unique_hours = list(df.hour.unique())
        unique_hours.sort()
        self.assertEqual([0, 1, 2, 3, 4, 20, 21, 22, 23], unique_hours)
        # Also check that all the dates are still there
        self.assertTrue(3 in df.day and 2 in df.day and 1 in df.day)

    def test_default_dates(self):
        """
        Test whether not passing a start and/or stop date will
        default to the min and/or max dates in the table.
        """
        sd = EventTableSubset(None, "2016-01-04")
        df = sd.get_dataframe()

        minimum = df["datetime"].min().to_pydatetime()
        min_comparison = pytz.timezone("Etc/UTC").localize(
            datetime(2016, 1, 1, 0, 0, 0)
        )
        self.assertTrue(minimum.timestamp() > min_comparison.timestamp())

        sd = EventTableSubset("2016-01-04", None, hours=(20, 5))
        df = sd.get_dataframe()

        maximum = df["datetime"].max().to_pydatetime()
        max_comparison = pytz.timezone("Etc/UTC").localize(
            datetime(2016, 1, 8, 0, 0, 0)
        )
        self.assertTrue(maximum.timestamp() < max_comparison.timestamp())

    def test_explain(self):
        """
        EventTableSubset().explain() method returns a string
        """

        # Usually not a critical function, so let's simply test by
        # asserting that it returns a string
        sd = EventTableSubset("2016-01-01", "2016-01-02")
        explain_string = sd.explain()
        self.assertIs(type(explain_string), str)
        self.assertIs(type(sd.explain(analyse=True)), str)

    def test_avoids_searching_extra_tables(self):
        """
        EventTableSubset() query doesn't look in additional partitioned tables.
        """
        sd = EventTableSubset("2016-01-01", "2016-01-02")
        explain_string = sd.explain()
        self.assertNotIn("calls_20160103", explain_string)
