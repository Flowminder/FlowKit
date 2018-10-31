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


class test_caching(TestCase):
    """
    Test that the objects can retain the data frame
    """

    def setUp(self):
        self.sd = EventTableSubset("2016-01-01", "2016-01-02")
        self.sd.get_dataframe()

    def test_turn_off_caching(self):
        """
        *.turn_off_caching() 'forgets' generated dataframe.
        """
        self.sd.turn_off_caching()
        with self.assertRaises(AttributeError):
            self._df

    def test_turn_off_caching_handles_error(self):
        """
        *.turn_off_caching() handles AttributeError.
        """
        self.sd.turn_off_caching()
        self.sd.turn_on_caching()
        self.sd.get_dataframe()

        del self.sd._df
        self.sd.turn_off_caching()

    def test_get_df_without_caching(self):
        """
        *.get_dataframe() can still retrieve the dataframe without caching.
        """
        self.sd.turn_off_caching()
        self.assertIs(type(self.sd.get_dataframe()), pd.DataFrame)
        self.assertIs(type(self.sd.get_dataframe()), pd.DataFrame)

    def test_turn_on_caching(self):
        """
        *.get_dataframe() datafrme is retained when we turning on caching.
        """
        self.sd.turn_off_caching()
        self.sd.turn_on_caching()
        self.sd.get_dataframe()
        self.assertIs(type(self.sd._df), pd.DataFrame)

    def test_cache_is_returned(self):
        """
        Cache property is returned when called.
        """
        self.sd.turn_on_caching()
        self.assertTrue(self.sd.cache)

        self.sd.turn_off_caching()
        self.assertFalse(self.sd.cache)


def test_unique_subscriber_column_names():
    """Test that column_names property of UniqueSubscribers is accurate"""
    us = UniqueSubscribers("2016-01-01", "2016-01-02")
    assert us.head(0).columns.tolist() == us.column_names


class test_unique_subscribers(TestCase):
    def setUp(self):

        self.UU = UniqueSubscribers("2016-01-01", "2016-01-02")

    def test_returns_set(self):
        """
        UniqueSubscribers() returns set.
        """
        self.assertIs(type(self.UU.as_set()), set)

    def test_subscribers_unique(self):
        """
        Returned dataframe has unique subscribers.
        """

        self.assertTrue(self.UU.get_dataframe()["subscriber"].is_unique)


class test_last_location(TestCase):
    """
    Tests for the LastLocation Class
    """

    def test_last_location_returns_df(self):
        """
        LastLocation() is able to return a dataframe.
        """

        last_loc = LastLocation("2016-01-01", "2016-01-02", level="admin3")
        df = last_loc.get_dataframe()
        self.assertIs(type(df), pd.DataFrame)

    def test_last_loc_vsite(self):
        """
        LastLocation() returns correct last location.
        """

        last_loc = LastLocation("2016-01-01", "2016-01-02", level="versioned-site")

        df = last_loc.get_dataframe()
        df.set_index("subscriber", inplace=True)
        self.assertEqual(df.ix["038OVABN11Ak4W5P"].site_id, "QeBRM8")
        self.assertEqual(df.ix["zGWn8opVmOQAD6xY"].site_id, "dJb0Wd")

    def test_last_loc_lat_lon(self):
        """
        LastLocation() can make queries at the lat-lon level.
        """

        last_loc = LastLocation("2016-01-01", "2016-01-02", level="lat-lon")
        df = last_loc.get_dataframe()
        df.set_index("subscriber", inplace=True)
        self.assertAlmostEqual(
            float(df.ix["yqw50eNyEwOxNDGL"].lat), 29.135638957790576, places=6
        )
        self.assertAlmostEqual(
            float(df.ix["yqw50eNyEwOxNDGL"].lon), 83.09669810947962, places=6
        )


class test_most_frequent_location(TestCase):
    def test_returns_df(self):
        """
        MostFrequentLocations().get_dataframe() returns a dataframe.
        """
        mfl = MostFrequentLocation("2016-01-01", "2016-01-02", level="admin3")
        df = mfl.get_dataframe()
        self.assertIs(type(df), pd.DataFrame)

    def test_vsites(self):
        """
        MostFrequentLocation() returns the correct locations.
        """

        mfl = MostFrequentLocation("2016-01-01", "2016-01-02", level="versioned-site")
        df = mfl.get_dataframe()
        df.set_index("subscriber", inplace=True)

        self.assertEqual(df.ix["yqQ6m96rp09dn510"].site_id, "wzrXjw")
        self.assertEqual(df.ix["zvaOknzKbEVD2eME"].site_id, "qvkp6J")

    def test_lat_lons(self):
        """
        MostFrequentLocations() has the correct values at the lat-lon level.
        """

        mfl = MostFrequentLocation("2016-01-01", "2016-01-02", level="lat-lon")
        df = mfl.get_dataframe()
        df.set_index("subscriber", inplace=True)

        self.assertAlmostEqual(float(df.ix["1QBlwRo4Kd5v3Ogz"].lat), 28.941925079951545)
        self.assertAlmostEqual(float(df.ix["1QBlwRo4Kd5v3Ogz"].lon), 82.61895799084449)

    def test_most_fequent_admin(self):
        """
        Test that the most frequent admin3 is correctly calculated.
        """

        mfl = locate_subscribers(
            "2016-01-01", "2016-01-02", level="admin3", method="most-common"
        )
        df = mfl.get_dataframe()
        # A few hand picked values
        df_set = df.set_index("subscriber")["name"]
        self.assertEqual(df_set["0gmvwzMAYbz5We1E"], "Dolpa")
        self.assertEqual(df_set["1QBlwRo4Kd5v3Ogz"], "Rukum")
        self.assertEqual(df_set["2Dq97XmPqvL6noGk"], "Arghakhanchi")


class test_aggregate_locations(TestCase):
    """
    Tests for the aggregation methods of the locations
    classes
    """

    def test_can_be_aggregated_admin3(self):
        """
        Query can be aggregated to a spatial level with admin3 data.
        """
        mfl = locate_subscribers(
            "2016-01-01", "2016-01-02", level="admin3", method="most-common"
        )
        agg = mfl.aggregate()
        df = agg.get_dataframe()
        self.assertIs(type(df), pd.DataFrame)
        self.assertEqual(list(df.columns), ["name", "total"])

    def test_can_be_aggregated_latlong(self):
        """
        Query can be aggregated to a spatial level with lat-lon data.
        """
        hl = HomeLocation(
            *[
                daily_location(d, level="lat-lon", method="last")
                for d in list_of_dates("2016-01-01", "2016-01-03")
            ]
        )
        agg = hl.aggregate()
        df = agg.get_dataframe()
        self.assertIs(type(df), pd.DataFrame)
        self.assertEqual(list(df.columns), ["lat", "lon", "total"])

    def test_joined_aggregate(self):
        """
        Test join aggregate.
        """
        mfl = MostFrequentLocation("2016-01-01", "2016-01-04", level="admin3")
        joined = mfl.join_aggregate(RadiusOfGyration("2016-01-01", "2016-01-04"))
        self.assertAlmostEqual(
            joined.get_dataframe().set_index("name").ix["Rasuwa"].rog, 199.956021886114
        )

    def test_joined_modal_aggregate(self):
        """
        Test join with modal aggregate.
        """
        mfl = MostFrequentLocation("2016-01-01", "2016-01-04", level="admin3")
        rog = SubscriberDegree("2016-01-01", "2016-01-04")
        joined = mfl.join_aggregate(rog, method="mode")
        rawus_mode = (
            rog.get_dataframe()
            .set_index("subscriber")
            .join(mfl.get_dataframe().set_index("subscriber"))
            .set_index("name")
            .ix["Rasuwa"]
            .degree.mode()[0]
        )
        self.assertAlmostEqual(
            joined.get_dataframe().set_index("name").ix["Rasuwa"].degree, rawus_mode
        )

    def test_joined_median_aggregate(self):
        """
        Test join with median aggregate.
        """
        mfl = MostFrequentLocation("2016-01-01", "2016-01-04", level="admin3")
        rog = RadiusOfGyration("2016-01-01", "2016-01-04")
        joined = mfl.join_aggregate(rog, method="median")
        rawus_avg = (
            rog.get_dataframe()
            .set_index("subscriber")
            .join(mfl.get_dataframe().set_index("subscriber"))
            .set_index("name")
            .ix["Rasuwa"]
            .rog.median()
        )
        self.assertAlmostEqual(
            joined.get_dataframe().set_index("name").ix["Rasuwa"].rog, rawus_avg
        )

    def test_joined_agg_date_mismatch(self):
        """
        Test that join aggregate with mismatched dates raises a warning.
        """
        mfl = MostFrequentLocation("2016-01-01", "2016-01-04", level="admin3")
        with self.assertWarns(UserWarning):
            joined = mfl.join_aggregate(RadiusOfGyration("2016-01-02", "2016-01-04"))

        with self.assertWarns(UserWarning):
            joined = mfl.join_aggregate(RadiusOfGyration("2016-01-01", "2016-01-05"))

    def test_joined_agg_hours_mismatch(self):
        """
        Test that join aggregate with mismatched hours doesn't warn.
        """
        mfl = MostFrequentLocation("2016-01-01 10:00", "2016-01-04", level="admin3")
        with warnings.catch_warnings(record=True) as w:
            joined = mfl.join_aggregate(RadiusOfGyration("2016-01-01", "2016-01-04"))
            self.assertFalse(w)


class test_dailylocations(TestCase):
    """
    Test for daily locations
    """

    def setUp(self):
        """
        Method that sets-up the test environment.
        Here we are also creating tables at every
        test and deleting it immediately after.
        """

        self.engine = Query.connection.engine

        self._create_test_schema()

    def _create_test_schema(self):
        """
        Private method that creates a test schema.
        """
        q = """
            CREATE SCHEMA IF NOT EXISTS daily_locations;
        """
        self.engine.execute(q)
        self.engine.commit()

    def _drop_test_schema(self):
        """
        Private method for dropping schema.
        """
        q = """
            DROP SCHEMA IF EXISTS daily_locations CASCADE;
        """
        self.engine.execute(q)
        self.engine.commit()

    def tearDown(self):
        """
        Method that drops test schema.
        """
        self._drop_test_schema()

    def test_can_return_df(self):
        """
        daily_location() can be initialised and return a dataframe.
        """

        dl = daily_location("2016-01-01")
        self.assertIs(type(dl.get_dataframe()), pd.DataFrame)

    def test_equivalent_to_locate_subscribers(self):
        """
        daily_location() is equivalent to the MostFrequentLocation().
        """
        mfl = MostFrequentLocation("2016-01-01", "2016-01-02")
        mfl_df = mfl.get_dataframe()

        dl = daily_location("2016-01-01", method="most-common")
        dl_df = dl.get_dataframe()

        self.assertTrue((dl_df == mfl_df).all().all())

    def test_equivalent_to_locate_subscribers_with_time(self):
        """
        daily_location() is equivalent to the MostFrequentLocation() with timestamps.
        """
        mfl = MostFrequentLocation("2016-01-01 18:00:00", "2016-01-02 06:00:00")
        mfl_df = mfl.get_dataframe()

        dl = daily_location(
            "2016-01-01 18:00:00", stop="2016-01-02 06:00:00", method="most-common"
        )
        dl_df = dl.get_dataframe()

        self.assertTrue((dl_df == mfl_df).all().all())

    def test_can_be_stored(self):
        """
        We can store a daily_location() in the right place.
        """

        schema = "cache"
        dl = daily_location("2016-01-01", level="cell")
        table_name = dl.table_name.split(".")[1]
        dl.store().result()
        self.assertTrue(dl.connection.has_table(table_name, schema=schema))
        dl = daily_location("2016-01-01", level="cell")
        self.assertTrue(table_name in dl.get_query())
        dl.invalidate_db_cache()

    def test_storing_updates(self):
        """
        Storing updates the query after storing.
        """

        dl = daily_location("2016-01-01", level="cell")
        dl.store().result()
        sql = dl.get_query()
        self.assertTrue(sql.startswith("SELECT * FROM cache"))
        dl.invalidate_db_cache()

    def test_force(self):
        """
        We can force an overwrite of the table.
        """
        dl = daily_location("2016-01-01", level="cell")
        dl.store().result()
        sql = "DELETE FROM {}".format(dl.table_name)
        dl.connection.engine.execute(sql)
        # This should not raise an error
        dl.store().result()
        self.assertTrue(len(dl) == 0)
        dl.store(force=True)
        self.assertTrue(len(dl.head()) != 0)
        dl.invalidate_db_cache()

    def test_non_standard_params(self):
        """
        Non-standard parameters don't trigger reading the cache.
        """
        dl = daily_location("2016-01-01", level="cell")
        table_name = dl.table_name
        dl.store().result()
        dl2 = daily_location("2016-01-01", level="cell", method="most-common")
        self.assertFalse(table_name in dl2.get_query())
        dl.invalidate_db_cache()

    def test_works_with_pcods(self):
        """
        We can get daily locations with p-codes rather than the standard names.
        """

        dl = daily_location("2016-01-05", level="admin3", column_name="admin3pcod")
        df = dl.get_dataframe()
        self.assertTrue(df.admin3pcod[0].startswith("524"))

    def test_hours(self):
        """
        Test that daily locations handles the hours parameter
        """

        # Lower level test test that subsetdates handles this correctly
        # we're just testing that it is passed on in this case.

        dl1 = daily_location("2016-01-01", level="cell")
        dl2 = daily_location("2016-01-01", level="cell", hours=(19, 23))
        dl3 = daily_location("2016-01-01", level="cell", hours=(19, 20))

        self.assertTrue(len(dl1) > len(dl2) > len(dl3))


class test_spanned_home_locations(TestCase):
    def setUp(self):
        self.dls = [
            daily_location(
                "2016-01-01 18:00:00", stop="2016-01-02 06:00:00", method="most-common"
            ),
            daily_location(
                "2016-01-02 18:00:00", stop="2016-01-03 06:00:00", method="most-common"
            ),
            daily_location(
                "2016-01-03 18:00:00", stop="2016-01-04 06:00:00", method="most-common"
            ),
        ]

    def test_inferred_start(self):
        """
        The start datetime is correctly inferred from a list of locations.
        """
        hl = HomeLocation(*self.dls)
        self.assertTrue(hl.start == "2016-01-01 18:00:00")

    def test_inferred_start_shuffled(self):
        """
        The start datetime is correctly inferred from a disordered list of locations.
        """
        hl = HomeLocation(*self.dls[::-1])
        self.assertTrue(hl.start == "2016-01-01 18:00:00")


class test_home_locations(TestCase):
    def setUp(self):

        self.hl = HomeLocation(
            *[daily_location(d) for d in list_of_dates("2016-01-01", "2016-01-03")]
        )
        self.hdf = self.hl.get_dataframe()
        self.hdf = self.hl.get_dataframe().set_index("subscriber")

    def test_returns_df(self):
        """
        HomeLocation() can return a dataframe.
        """
        self.assertIs(type(self.hdf), pd.DataFrame)

    def test_selected_values(self):
        """
        HomeLocation() values are correct.
        """
        self.assertEquals(self.hdf.ix["038OVABN11Ak4W5P"][0], "Dolpa")
        self.assertEquals(self.hdf.ix["E1n7JoqxPBjvR5Ve"][0], "Baglung")
        self.assertEquals(self.hdf.ix["gkBLe0mN5j3qmRpX"][0], "Myagdi")
        self.assertEquals(self.hdf.ix["5Kgwy8Gp6DlN3Eq9"][0], "Kapilbastu")


class test_missing_dates(TestCase):
    """
    Tests that deal with the logic for dealing with missing dates.
    """

    def test_daily_locs_errors(self):
        """
        daily_location() errors when we ask for a date that does not exist.
        """

        with self.assertRaises(MissingDateError):
            dl = daily_location("2016-01-31")


def test_subscriber_location_subset_column_names(exemplar_level_param):
    ss = SubscriberLocationSubset(
        "2016-01-01", "2016-01-07", min_calls=1, **exemplar_level_param
    )
    assert ss.head(0).columns.tolist() == ss.column_names


class test_subscriber_location_subset(TestCase):
    def test_subscribers_make_atleast_one_call_in_admin0(self):
        """
        The set of subscribers who make at least one call within admin0 over
        whole test time period should be equal to set of unique subscribers
        in test calls table.
        """

        start, stop = "2016-01-01", "2016-01-07"

        sls = SubscriberLocationSubset(start, stop, min_calls=1, level="admin0")
        us = UniqueSubscribers(start, stop, table="events.calls")

        sls_subs = set(sls.get_dataframe()["subscriber"])
        us_subs = set(us.get_dataframe()["subscriber"])

        self.assertEquals(sls_subs, us_subs)

    def test_subscribers_who_make_atleast_3_calls_in_central_development_region(self):
        """
        Test that we can find subsets for multiple geometries at same time. Will
        find subscribers who have made at least 2 calls in any of the admin2 regions
        within Central Development admin1 region.
        """
        start, stop = "2016-01-01", "2016-01-07"
        regions = Table("admin2", "geography").subset(
            "admin1name", ["Central Development Region"]
        )

        sls = SubscriberLocationSubset(
            start,
            stop,
            min_calls=2,
            level="polygon",
            column_name="admin2pcod",
            polygon_table=regions,
        )

        df = sls.get_dataframe()

        # we have results for multiple regions
        self.assertGreater(len(df.admin2pcod.unique()), 1)

        # some users should have have made at least 2 calls in more than one region
        # and should therefore appear twice
        self.assertGreater(len(df[df.duplicated("subscriber")]), 0)
