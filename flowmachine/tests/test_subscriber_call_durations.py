# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest import TestCase
from flowmachine.features.subscriber.subscriber_call_durations import *
import pytest


@pytest.mark.parametrize(
    "query",
    [
        SubscriberCallDurations,
        PairedPerLocationSubscriberCallDurations,
        PerLocationSubscriberCallDurations,
        PairedSubscriberCallDurations,
    ],
)
@pytest.mark.parametrize("stat", valid_stats)
def test_subscriber_call_durations_column_names(query, exemplar_level_param, stat):
    """ Test that column_names property matches head(0)"""
    query_instance = query(
        "2016-01-01", "2016-01-07", **exemplar_level_param, statistic=stat
    )
    assert query_instance.head(0).columns.tolist() == query_instance.column_names


class CallDurationsTestCase(TestCase):
    def setUp(self):
        self.out_durations = SubscriberCallDurations("2016-01-01", "2016-01-07")
        self.paired_durations = PairedSubscriberCallDurations(
            "2016-01-01", "2016-01-07"
        )
        self.per_location_durations = PerLocationSubscriberCallDurations(
            "2016-01-01", "2016-01-07"
        )
        self.paired_per_location_durations = PairedPerLocationSubscriberCallDurations(
            "2016-01-01", "2016-01-07"
        )

    def test_polygon_tables(self):
        """Test that custom polygons can be used."""
        per_location_durations = PerLocationSubscriberCallDurations(
            "2016-01-01",
            "2016-01-07",
            level="polygon",
            polygon_table="geography.admin3",
            column_name="admin3name",
        )
        df = per_location_durations.get_dataframe()

        self.assertEqual(
            df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum, 12281
        )
        df = PerLocationSubscriberCallDurations(
            "2016-01-01", "2016-01-07", direction="in"
        ).get_dataframe()
        self.assertEqual(
            df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum, 24086
        )
        df = PerLocationSubscriberCallDurations(
            "2016-01-01", "2016-01-07", direction="both"
        ).get_dataframe()
        self.assertEqual(
            df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum,
            24086 + 12281,
        )

        paired_per_location_durations = PairedPerLocationSubscriberCallDurations(
            "2016-01-01",
            "2016-01-07",
            level="polygon",
            polygon_table="geography.admin3",
            column_name="admin3name",
        )

        df = paired_per_location_durations.get_dataframe()
        self.assertEqual(
            df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum, 12281
        )
        self.assertEqual(
            df.groupby("msisdn_counterpart").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum,
            24086,
        )

    def test_durations(self):
        """Test some hand picked durations"""
        df = self.out_durations.get_dataframe().set_index("subscriber")
        self.assertEqual(df.loc["nL9KYGXpz2G5mvDa"].duration_sum, 12281)
        df = (
            SubscriberCallDurations("2016-01-01", "2016-01-07", direction="in")
            .get_dataframe()
            .set_index("subscriber")
        )
        self.assertEqual(df.loc["nL9KYGXpz2G5mvDa"].duration_sum, 24086)
        df = (
            SubscriberCallDurations("2016-01-01", "2016-01-07", direction="both")
            .get_dataframe()
            .set_index("subscriber")
        )
        self.assertEqual(df.loc["nL9KYGXpz2G5mvDa"].duration_sum, 24086 + 12281)

    def test_paired_durations(self):
        """Test paired durations sum to the same as in/out durations"""
        df = self.paired_durations.get_dataframe()
        self.assertEqual(
            df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum, 12281
        )
        self.assertEqual(
            df.groupby("msisdn_counterpart").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum,
            24086,
        )

    def test_per_location_durations(self):
        """Test per location durations sums to the same as in/out durations"""
        df = self.per_location_durations.get_dataframe()
        self.assertEqual(
            df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum, 12281
        )
        df = PerLocationSubscriberCallDurations(
            "2016-01-01", "2016-01-07", direction="in"
        ).get_dataframe()
        self.assertEqual(
            df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum, 24086
        )
        df = PerLocationSubscriberCallDurations(
            "2016-01-01", "2016-01-07", direction="both"
        ).get_dataframe()
        self.assertEqual(
            df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum,
            24086 + 12281,
        )

    def test_paired_per_location_durations(self):
        """Test paired per location durations sum to the same as in/out durations"""
        df = self.paired_per_location_durations.get_dataframe()
        self.assertEqual(
            df.groupby("subscriber").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum, 12281
        )
        self.assertEqual(
            df.groupby("msisdn_counterpart").sum().loc["nL9KYGXpz2G5mvDa"].duration_sum,
            24086,
        )

    def test_direction_checks(self):
        """Test that bad direction params are rejected"""
        with self.assertRaises(ValueError):
            PerLocationSubscriberCallDurations(
                "2016-01-01", "2016-01-07", direction="alf"
            )
        with self.assertRaises(ValueError):
            SubscriberCallDurations("2016-01-01", "2016-01-07", direction="mooses")

    def test_long_runtime_warning(self):
        """Test that a warning about potentially long runtime is raised."""
        with self.assertWarns(UserWarning):
            PairedPerLocationSubscriberCallDurations("2016-01-01", "2016-01-07")
