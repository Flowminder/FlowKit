# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for Subscriber Location Clustering methods. Those methods cluster
location based on different methods, offering good options for
reducing the dimensionality of the problem.
"""
import numpy as np
import pandas as pd

from unittest import TestCase

import pytest
from shapely.geometry import box, MultiPoint

from flowmachine.core import Table, CustomQuery
from flowmachine.core.query import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features import (
    CallDays,
    HartiganCluster,
    SubscriberLocationCluster,
    EventScore,
)


@pytest.mark.usefixtures("skip_datecheck")
def test_hartigan_column_names():
    """Test that Hartigan has correct column_names property."""
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")
    hartigan = HartiganCluster(cd, 50)
    assert hartigan.head(0).columns.tolist() == hartigan.column_names


@pytest.mark.usefixtures("skip_datecheck")
def test_joined_hartigan_column_names():
    """Test that Hartigan has correct column_names property."""
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")
    hartigan = HartiganCluster(cd, 50)
    es = EventScore(start="2016-01-01", stop="2016-01-05", level="versioned-site")
    joined = hartigan.join_to_cluster_components(es)
    assert joined.head(0).columns.tolist() == joined.column_names


def test_hartigan_type_error():
    """Test that Hartigan errors if given a non-query like calldays"""
    with pytest.raises(TypeError):
        HartiganCluster("fudge", 50)


@pytest.mark.usefixtures("skip_datecheck")
def test_joined_hartigan_type_error():
    """Test that joining hartigan to something which isn't query like raises a type error."""
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")
    hartigan = HartiganCluster(cd, 50)
    with pytest.raises(TypeError):
        hartigan.join_to_cluster_components("banana")


class Sites(GeoDataMixin, Query):
    """
    Selects the geometric coordinates of the versioned-sites.
    """

    def _make_query(self):

        return "SELECT id AS site_id, version, geom_point FROM infrastructure.sites"

    def _geo_augmented_query(self):
        return (
            (
                "SELECT id AS site_id, version, geom_point as geom,  row_number() over() as gid FROM infrastructure.sites"
            ),
            ["site_id", "version", "geom", "gid"],
        )


def get_geom_point(x, sites):
    geom_points = []
    for site_id, version in zip(*[x.site_id, x.version]):
        geom_point = sites.query(
            "site_id == @site_id & version == @version"
        ).geometry.values[0]
        geom_points.append(geom_point)
    return MultiPoint(geom_points)


class TestHartiganCluster(TestCase):
    def setUp(self):
        self.cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")
        self.cd_df = self.cd.get_dataframe()

        self.hartigan = HartiganCluster(self.cd, 50)
        self.har_df = self.hartigan.to_geopandas()

    def tearDown(self):
        [cd.invalidate_db_cache() for cd in CallDays.get_stored()]

    def test_returns_dataframe(self):
        """
        Tests that it returns a dataframe with the correct columns
        """
        self.assertIsInstance(self.har_df, pd.DataFrame)
        self.assertEquals(
            set(self.har_df.columns),
            set(
                [
                    "subscriber",
                    "geometry",
                    "rank",
                    "calldays",
                    "site_id",
                    "version",
                    "centroid",
                ]
            ),
        )

    def test_cluster_is_within_envelope(self):
        """
        Test that all the clusters are within the enveloped formed by all the towers in the cluster.
        """
        sites = Sites().to_geopandas()
        self.har_df["towers"] = self.har_df.apply(lambda x: get_geom_point(x, sites), 1)
        s = self.har_df.apply(
            lambda x: x.geometry.intersects(box(*x.towers.bounds)), axis=1
        )
        self.assertTrue(all(s))

    def test_first_call_day_in_first_cluster(self):
        """
        Test that the first ranked call day of each subscriber is in the first cluster of each subscriber.
        """
        cd_first = (
            self.cd_df[["subscriber", "site_id", "version"]]
            .groupby("subscriber")
            .first()
        )
        har_first = (
            self.har_df[["subscriber", "site_id", "version"]]
            .groupby("subscriber")
            .first()
        )

        joined = cd_first.join(har_first, lsuffix="_cd", rsuffix="_har")
        s = joined.apply(
            lambda x: (x.site_id_cd in x.site_id_har)
            and (x.version_cd in x.version_har),
            axis=1,
        )
        self.assertTrue(all(s))

    def test_bigger_radius_yields_fewer_clusters(self):
        """
        Test whether bigger radius yields fewer clusters per subscriber
        """
        radius = [1, 2, 5, 10, 50]

        h = HartiganCluster(self.cd, radius[0]).get_dataframe()
        nclusters_small_radius = h.groupby("subscriber").size()

        for r in radius[1:]:
            h = HartiganCluster(self.cd, r).get_dataframe()
            nclusters_big_radius = h.groupby("subscriber").size()
            self.assertTrue(all(nclusters_small_radius >= nclusters_big_radius))
            nclusters_small_radius = nclusters_big_radius

    def test_different_call_days_format(self):
        """
        Test whether we can pass different call days format such as table name, SQL query and CallDays class.
        """
        cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")
        har = HartiganCluster(cd, 50).get_dataframe()
        self.assertIsInstance(har, pd.DataFrame)

        cd.store().result()

        har = HartiganCluster(Table(cd.table_name), 50).get_dataframe()
        self.assertIsInstance(har, pd.DataFrame)

        cd_query = cd.get_query()
        har = HartiganCluster(CustomQuery(cd_query), 50).get_dataframe()
        self.assertIsInstance(har, pd.DataFrame)

    def test_call_threshold_works(self):
        """
        Test whether a call threshold above 1 limits the number of clusters.
        """
        self.assertTrue(any(self.har_df.calldays == 1))
        har = HartiganCluster(self.cd, 50, call_threshold=2).get_dataframe()
        self.assertFalse(all(self.har_df.calldays > 1))

    def test_buffered_hartigan(self):
        """
        Test whether Hartigan produces buffered clusters when buffer is larger than 0.
        """
        har = HartiganCluster(self.cd, 50, buffer=2).to_geopandas()
        areas = har.geometry.area
        # since the mock data does not have geom_area in the site table we either
        # get the clusters with area equivalent to 2km2 (areas below are in degrees) or None.
        min_area = areas.min()
        max_area = areas.max()
        self.assertAlmostEquals(min_area, 0.001, 3)
        self.assertAlmostEquals(max_area, 0.001, 3)

    def test_all_options_hartigan(self):
        """
        Test whether Hartigan works when changing all options.
        """
        har = HartiganCluster(self.cd, 50, buffer=2, call_threshold=2).to_geopandas()
        self.assertIsInstance(har, pd.DataFrame)
        self.assertEquals(
            set(har.columns),
            set(
                [
                    "subscriber",
                    "geometry",
                    "rank",
                    "calldays",
                    "site_id",
                    "version",
                    "centroid",
                ]
            ),
        )

    def test_join_returns_the_same_clusters(self):
        """
        Test whether joining to another table for which the start and stop time are the same yields the same clusters.
        """
        es = EventScore(start="2016-01-01", stop="2016-01-04", level="versioned-site")

        joined = (
            self.hartigan.join_to_cluster_components(es)
            .to_geopandas()
            .sort_values(["subscriber", "rank", "calldays"])
        )
        joined.reset_index(inplace=True, drop=True)

        self.har_df.sort_values(["subscriber", "rank", "calldays"], inplace=True)
        self.har_df.reset_index(inplace=True, drop=True)

        cols = ["subscriber", "geometry", "rank", "calldays"]
        compare = joined[cols] == self.har_df[cols]
        self.assertTrue(all(compare.all()))


class TestSubscriberLocationCluster(TestCase):
    def tearDown(self):
        [cd.invalidate_db_cache() for cd in CallDays.get_stored()]

    def test_unlisted_methods_raises_error(self):
        """
        Test whether unlisted methods raise error
        """
        with self.assertRaises(ValueError):
            SubscriberLocationCluster(
                method="not_listed", start="2016-01-01", stop="2016-01-04"
            )

    def test_lack_of_radius_with_hartigan_raises_error(self):
        """
        Test whether not passing a radius raises when choosing `hartigan` as a method raises an error
        """
        with self.assertRaises(NameError):
            SubscriberLocationCluster(
                method="hartigan", start="2016-01-01", stop="2016-01-04"
            )

    def test_hartigan_runs_without_error(self):
        """
        Test whether hartigan runs with correct arguments without failing.
        """
        har = SubscriberLocationCluster(
            method="hartigan", start="2016-01-01", stop="2016-01-04", radius=2
        )

        self.assertIsInstance(har.get_dataframe(), pd.DataFrame)
