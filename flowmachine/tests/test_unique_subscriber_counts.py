# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the UniqueSubscriberCounts query 
"""
import pandas as pd
import pytest

# since the directory is not a module, it cannot be imported
# we use this temporary hack for sourcing utils.py while the testing framework is not completely refactored
import os

exec(open(os.path.join(os.path.dirname(__file__), "utils.py")).read())
from unittest import TestCase
from flowmachine.features import UniqueSubscriberCounts
from flowmachine.features.utilities import subscriber_locations
from flowmachine.core.errors import BadLevelError


@pytest.mark.usefixtures("skip_datecheck")
def test_unique_subscriber_counts_column_names(exemplar_level_param):
    """
    Test that column_names property of UniqueSubscriberCounts matches head(0)
    """
    usc = UniqueSubscriberCounts("2016-01-01", "2016-01-04", **exemplar_level_param)
    assert usc.head(0).columns.tolist() == usc.column_names


class test_unique_subscriber_counts(TestCase):
    """
    Tests associated with the unique_location_counts class
    """

    def setUp(self):
        self.usc = UniqueSubscriberCounts(
            "2016-01-01", "2016-01-02", level="cell", hours=(5, 17)
        )

    def test_returns_df(self):
        """
        UniqueLocationCounts() object can return a dataframe
        """
        self.assertIs(type(self.usc.get_dataframe()), pd.DataFrame)

    def test_returns_errors(self):
        """
        Test level exists
        """
        with self.assertRaises(BadLevelError):
            UniqueSubscriberCounts("2016-01-01", "2016-01-02", level="foo")

    def test_handles_levels(self):
        """
        Location classes can be called with any level
        and return a dataframe with the right columns.
        """
        expected_cols = [c + ["dl_count"] for c in subscriber_plus_levels]
        sweep_levels_assert_columns(
            UniqueSubscriberCounts,
            {"start": "2016-01-01", "stop": "2016-01-02"},
            expected_cols=[i + ["unique_subscriber_counts"] for i in levels_only],
        )

    def test_correct_counts(self):
        """
        UniqueLocationCounts returns correct counts.
        """
        df = self.usc.get_dataframe()
        dful = subscriber_locations(
            "2016-01-01", "2016-01-02", level="cell", hours=(5, 17)
        ).get_dataframe()
        self.assertEqual(
            [
                df["unique_subscriber_counts"][0],
                df["unique_subscriber_counts"][1],
                df["unique_subscriber_counts"][2],
            ],
            [
                len(
                    dful[dful["location_id"] == df["location_id"][0]][
                        "subscriber"
                    ].unique()
                ),
                len(
                    dful[dful["location_id"] == df["location_id"][1]][
                        "subscriber"
                    ].unique()
                ),
                len(
                    dful[dful["location_id"] == df["location_id"][2]][
                        "subscriber"
                    ].unique()
                ),
            ],
        )
