# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the subscriber subsetting functionality
"""


from unittest import TestCase

import pytest

from flowmachine.core import Table
from flowmachine.features import (
    RadiusOfGyration,
    HomeLocation,
    UniqueSubscribers,
    daily_location,
)
from flowmachine.features.utilities.sets import EventTableSubset
from flowmachine.utils import list_of_dates


@pytest.mark.parametrize(
    "columns", [["msisdn"], ["*"], ["id", "msisdn"]], ids=lambda x: f"{x}"
)
def test_events_table_subset_column_names(columns):
    """Test that EventTableSubset column_names property is accurate."""
    etu = EventTableSubset(
        "2016-01-01", "2016-01-02", columns=columns, tables=["events.calls"]
    )
    assert etu.head(0).columns.tolist() == etu.column_names


@pytest.mark.parametrize("ident", ("msisdn", "imei", "imsi"))
def test_events_table_subscriber_ident_substitutions(ident):
    """Test that EventTableSubset replaces the subscriber ident column name with subscriber."""
    etu = EventTableSubset(
        "2016-01-01",
        "2016-01-02",
        columns=[ident],
        tables=["events.calls"],
        subscriber_identifier=ident,
    )
    assert "subscriber" == etu.head(0).columns[0]
    assert ["subscriber"] == etu.column_names


class test_subscriber_subset(TestCase):
    def setUp(self):
        #  This is a list of subscribers that I have pre selected
        self.subscriber_list = [
            "a7wRAnjb2znjOlxK",
            "Q7lRVD4YY1xawymG",
            "5Kgwy8Gp6DlN3Eq9",
            "BKMy1nYEZpnoEA7G",
            "NQV3J52PeYgbLm2w",
            "pD0me6Do0dNE1nYZ",
            "KyonmeOzDkEN40qP",
            "vBR2gwl2B0nDJob5",
            "AwQr1M3w8mMvV3p4",
        ]

    def test_cdrs_can_be_subset_by_table(self):
        """
        We can subset CDRs by a table in the database.
        """

        # Create a temporary table in the DB
        con = Table.connection.engine

        sql = "DROP TABLE IF EXISTS subscriber_list"
        con.execute(sql)

        sql = """CREATE TABLE subscriber_list (subscriber TEXT)"""
        con.execute(sql)

        formatted_subscribers = ",".join(
            "('{}')".format(u) for u in self.subscriber_list
        )
        sql = """INSERT INTO subscriber_list (subscriber) VALUES {}""".format(
            formatted_subscribers
        )
        con.execute(sql)
        su = EventTableSubset(
            "2016-01-01", "2016-01-03", subscriber_subset=Table("subscriber_list")
        )

        df = su.get_dataframe()
        sql = "DROP TABLE IF EXISTS subscriber_list"
        con.execute(sql)
        # Get the set of subscribers present in the dataframe, we need to handle the logic
        # of msisdn_from/msisdn_to
        calculated_subscriber_set = set(df.subscriber)

        self.assertEqual(calculated_subscriber_set, set(self.subscriber_list))

    def test_subset_correct(self):
        """Test that pushed in subsetting matches .subset result"""
        su = EventTableSubset(
            "2016-01-01", "2016-01-03", subscriber_subset=self.subscriber_list
        )
        subsu = EventTableSubset("2016-01-01", "2016-01-03").subset(
            "subscriber", self.subscriber_list
        )
        self.assertTrue(all(su.get_dataframe() == subsu.get_dataframe()))
        su = HomeLocation(
            *[
                daily_location(d, subscriber_subset=self.subscriber_list)
                for d in list_of_dates("2016-01-01", "2016-01-07")
            ]
        )
        subsu = HomeLocation(
            *[daily_location(d) for d in list_of_dates("2016-01-01", "2016-01-03")]
        ).subset("subscriber", self.subscriber_list)
        self.assertTrue(all(su.get_dataframe() == subsu.get_dataframe()))

    def test_query_can_be_subscriber_set_restricted(self):
        """Test that some queries can be limited to only a subset of subscribers."""

        # Create a temporary table in the DB
        con = Table.connection.engine

        sql = "DROP TABLE IF EXISTS subscriber_list"
        con.execute(sql)

        sql = """CREATE TABLE subscriber_list (subscriber TEXT)"""
        con.execute(sql)

        formatted_subscribers = ",".join(
            "('{}')".format(u) for u in self.subscriber_list
        )
        sql = """INSERT INTO subscriber_list (subscriber) VALUES {}""".format(
            formatted_subscribers
        )
        con.execute(sql)
        rog = RadiusOfGyration(
            "2016-01-01", "2016-01-03", subscriber_subset=Table("subscriber_list")
        )
        hl = HomeLocation(
            *[
                daily_location(d, subscriber_subset=Table("subscriber_list"))
                for d in list_of_dates("2016-01-01", "2016-01-03")
            ]
        )
        rog_df = rog.get_dataframe()
        hl_df = hl.get_dataframe()
        sql = "DROP TABLE IF EXISTS subscriber_list"
        con.execute(sql)

        # Get the set of subscribers present in the dataframe, we need to handle the logic
        # of msisdn_from/msisdn_to
        calculated_subscriber_set = set(rog_df.subscriber)

        self.assertEqual(calculated_subscriber_set, set(self.subscriber_list))
        calculated_subscriber_set = set(hl_df.subscriber)

        self.assertEqual(calculated_subscriber_set, set(self.subscriber_list))

    def test_cdrs_can_be_subset_by_list(self):
        """
        We can subset CDRs with a list.
        """

        su = EventTableSubset(
            "2016-01-01", "2016-01-03", subscriber_subset=self.subscriber_list
        )
        df = su.get_dataframe()

        # Get the set of subscribers present in the dataframe, we need to handle the logic
        # of msisdn_from/msisdn_to
        calculated_subscriber_set = set(df.subscriber)

        self.assertEqual(calculated_subscriber_set, set(self.subscriber_list))

    def test_can_subset_by_sampler(self):
        """Test that we can use the output of another query to subset by."""
        unique_subs_sample = UniqueSubscribers(
            "2016-01-01", "2016-01-07"
        ).random_sample(size=10, method="system", seed=0.1)
        su = EventTableSubset(
            "2016-01-01", "2016-01-03", subscriber_subset=unique_subs_sample
        )
        su_set = set(su.get_dataframe().subscriber)
        uu_set = set(unique_subs_sample.get_dataframe().subscriber)
        self.assertSetEqual(su_set, uu_set)
        self.assertEqual(len(su_set), 10)

    def test_ommitted_subscriber_column(self):
        """Test that a result is returned and warning is raised when ommitting a subscriber column."""
        with self.assertWarns(UserWarning):
            su_omit_col = EventTableSubset(
                "2016-01-01",
                "2016-01-03",
                subscriber_subset=self.subscriber_list,
                columns=["duration"],
            ).get_dataframe()
        su_all_cols = EventTableSubset(
            "2016-01-01",
            "2016-01-03",
            subscriber_subset=self.subscriber_list,
            columns=["msisdn", "duration"],
        ).get_dataframe()
        self.assertListEqual(
            su_omit_col.duration.values.tolist(), su_all_cols.duration.values.tolist()
        )
        self.assertListEqual(su_omit_col.columns.tolist(), ["duration"])

    def tearDown(self):
        UniqueSubscribers("2016-01-01", "2016-01-07").invalidate_db_cache()
