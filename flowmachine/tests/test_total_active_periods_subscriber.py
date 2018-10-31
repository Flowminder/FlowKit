# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the class flowmachine.TotalActivePeriodsSubscriber
"""

from unittest import TestCase
from flowmachine.features import TotalActivePeriodsSubscriber
from pandas import DataFrame
import matplotlib.pyplot as plt


class test_total_active_periods_subscriber(TestCase):
    def setUp(self):
        # Three periods of length one
        self.tap = TotalActivePeriodsSubscriber("2016-01-01", 3, 1)

    def test_returns_df(self):
        """
        flowmachine.TotalActivePeriodsSubscriber returns a dataframe
        """
        self.assertIs(type(self.tap.get_dataframe()), DataFrame)

    def test_certain_results(self):
        """
        flowmachine.TotalActivePeriodsSubscriber gives correct results.
        """

        # This subscriber should have only 2 active time periods
        subscriber_with_2 = "rmb4PG9gx1ZqBE01"
        # and this one three
        subscriber_with_3 = "dXogRAnyg1Q9lE3J"

        df = self.tap.get_dataframe().set_index("subscriber")
        self.assertEqual(df.ix[subscriber_with_2].active_periods, 2)
        self.assertEqual(df.ix[subscriber_with_3].active_periods, 3)
        self.assertEqual(df.ix[subscriber_with_2].inactive_periods, 1)
        self.assertEqual(df.ix[subscriber_with_3].inactive_periods, 0)

    def test_multiple_day_periods(self):
        """
        flowmachine.TotalActivePeriodsSubscriber can handle a period
        greater than one day.
        """

        tap = TotalActivePeriodsSubscriber("2016-01-02", 3, 2)
        df = tap.get_dataframe()
        starts = ["2016-01-02", "2016-01-04", "2016-01-06"]
        stops = ["2016-01-04", "2016-01-06", "2016-01-08"]
        # Check that the start and stop dates are as expected
        self.assertEquals(tap.starts, starts)
        self.assertEquals(tap.stops, stops)
        # For good measure assert that no subscriber has more than the
        # max number of periods
        self.assertEquals(df.active_periods.max(), 3)
        self.assertEquals(df.inactive_periods.max(), 0)

    def test_returns_matplotlib_axes(self):
        """
        flowmachine.TotalActivePeriodsSubscriber.plot returns a matplotlib
        axes object.
        """
        self.assertIsInstance(self.tap.plot(), plt.Axes)

    def test_raises_value_error_bad_unit(self):
        """
        flowmachine.TotalActivePeriodsSubscriber raises value error when
        we pass a none-allowed time unit.
        """

        with self.assertRaises(ValueError):
            TotalActivePeriodsSubscriber("2016-01-01", 5, period_unit="microfortnight")

    def test_non_standard_units(self):
        """
        flowmachine.TotalActivePeriodsSubscriber is able to handle a period_unit other
        than the default 'days'.
        """

        df = (
            TotalActivePeriodsSubscriber("2016-01-01", 5, period_unit="hours")
            .get_dataframe()
            .set_index("subscriber")
        )

        self.assertEqual(df.ix["VkzMxYjv7mYn53oK"].active_periods, 3)
        self.assertEqual(df.ix["DzpZJ2EaVQo2X5vM"].active_periods, 1)
        self.assertEqual(df.ix["VkzMxYjv7mYn53oK"].inactive_periods, 2)
        self.assertEqual(df.ix["DzpZJ2EaVQo2X5vM"].inactive_periods, 4)
