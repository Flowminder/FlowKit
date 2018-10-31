# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the spatial activity class
"""

# since the directory is not a module, it cannot be imported
# we use this temporary hack for sourcing utils.py while the testing framework is not completely refactored
import os

exec(open(os.path.join(os.path.dirname(__file__), "utils.py")).read())
from unittest import TestCase
from pandas import DataFrame

from flowmachine.features import TotalLocationEvents


class test_total_events_location(TestCase):
    def test_events_at_cell_level(self):
        """
        TotalLocationEvents() returns data at the level of the cell.
        """

        te = TotalLocationEvents("2016-01-01", "2016-01-04", level="versioned-site")
        df = te.get_dataframe()
        self.assertIs(type(df), DataFrame)

        # Test one of the values
        df.date = df.date.astype(str)
        val = list(
            df[
                (df.date == "2016-01-03") & (df.site_id == "zArRjg") & (df.hour == 17)
            ].total
        )[0]
        self.assertEqual(val, 3)

    def test_ignore_texts(self):
        """
        TotalLocationEvents() can get the total activity at cell level excluding texts.
        """
        te = TotalLocationEvents(
            "2016-01-01", "2016-01-04", level="versioned-site", table="events.calls"
        )
        df = te.get_dataframe()
        self.assertIs(type(df), DataFrame)
        # Test one of the values
        df.date = df.date.astype(str)
        val = list(
            df[
                (df.date == "2016-01-01") & (df.site_id == "0xqNDj") & (df.hour == 3)
            ].total
        )[0]
        self.assertEqual(val, 3)

    def test_only_incoming(self):
        """
        TotalLocationEvents() can get activity, ignoring outgoing calls.
        """
        te = TotalLocationEvents(
            "2016-01-01", "2016-01-04", level="versioned-site", direction="in"
        )
        df = te.get_dataframe()
        self.assertIs(type(df), DataFrame)
        # Test one of the values
        df.date = df.date.astype(str)
        val = list(
            df[
                (df.date == "2016-01-01") & (df.site_id == "6qpN0p") & (df.hour == 0)
            ].total
        )[0]
        self.assertEqual(val, 2)

    def test_events_daily(self):
        """
        TotalLocationEvents() can get activity on a daily level.
        """
        te = TotalLocationEvents(
            "2016-01-01", "2016-01-04", level="versioned-site", interval="day"
        )
        df = te.get_dataframe()
        self.assertIs(type(df), DataFrame)
        self.assertEqual(
            list(df.columns), ["site_id", "version", "lon", "lat", "date", "total"]
        )
        # Test one of the values
        df.date = df.date.astype(str)
        val = list(df[(df.date == "2016-01-03") & (df.site_id == "B8OaG5")].total)[0]
        self.assertEqual(val, 95)

    def test_events_min(self):
        """
        TotalLocationEvents() can get events on a min-by-min basis.
        """
        te = TotalLocationEvents(
            "2016-01-01", "2016-01-04", level="versioned-site", interval="min"
        )
        df = te.get_dataframe()
        self.assertIs(type(df), DataFrame)
        self.assertEqual(
            list(df.columns),
            ["site_id", "version", "lon", "lat", "date", "hour", "min", "total"],
        )
        # Test one of the values
        df.date = df.date.astype(str)
        val = list(
            df[
                (df.date == "2016-01-03")
                & (df.site_id == "zdNQx2")
                & (df.hour == 15)
                & (df["min"] == 20)
            ].total
        )[0]
        self.assertEqual(val, 1)

    def test_all_levels(self):
        """
        TotalLocationEvents() by day can calculate activity on all levels.
        """

        expected_cols = [c + ["date", "total"] for c in levels_only]
        sweep_levels_assert_columns(
            TotalLocationEvents,
            {"start": "2016-01-01", "stop": "2016-01-03", "interval": "day"},
            expected_cols=expected_cols,
            order_matters=False,
        )

    def test_all_levels_hour(self):
        """
        TotalLocationEvents() by hour can calculate activity on all levels.
        """
        expected_cols = [c + ["date", "total", "hour"] for c in levels_only]
        sweep_levels_assert_columns(
            TotalLocationEvents,
            {"start": "2016-01-01", "stop": "2016-01-03", "interval": "hour"},
            expected_cols=expected_cols,
            order_matters=False,
        )

    def test_all_levels_min(self):
        """
        TotalLocationEvents() by min can calculate activity on all levels.
        """
        expected_cols = [c + ["date", "total", "hour", "min"] for c in levels_only]
        sweep_levels_assert_columns(
            TotalLocationEvents,
            {"start": "2016-01-01", "stop": "2016-01-03", "interval": "min"},
            expected_cols=expected_cols,
            order_matters=False,
        )
