# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for Subscriber Location Clustering methods. Those methods cluster
location based on different methods, offering good options for
reducing the dimensionality of the problem.
"""
import pandas as pd


import pytest
from geopandas import GeoSeries
from shapely.geometry import box, MultiPoint

from flowmachine.core import Table, CustomQuery
from flowmachine.core.query import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features import (
    CallDays,
    HartiganCluster,
    subscriber_location_cluster,
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
    relevant_sites = sites.loc[zip(*[x.site_id, x.version])]
    return box(*MultiPoint(relevant_sites.geometry).bounds)


def test_cluster_is_within_envelope(get_dataframe):
    """
    Test that all the clusters are within the enveloped formed by all the towers in the cluster.
    """
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")

    hartigan = HartiganCluster(cd, 50)
    har_df = hartigan.to_geopandas()
    sites = Sites().to_geopandas().set_index(["site_id", "version"])
    towers = GeoSeries(har_df.apply(lambda x: get_geom_point(x, sites), 1))
    s = har_df.intersects(towers)
    assert all(s)


def test_first_call_day_in_first_cluster(get_dataframe):
    """
    Test that the first ranked call day of each subscriber is in the first cluster of each subscriber.
    """
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")
    cd_df = get_dataframe(cd)

    hartigan = HartiganCluster(cd, 50)
    har_df = hartigan.to_geopandas()
    cd_first = cd_df[["subscriber", "site_id", "version"]].groupby("subscriber").first()
    har_first = (
        har_df[["subscriber", "site_id", "version"]].groupby("subscriber").first()
    )

    joined = cd_first.join(har_first, lsuffix="_cd", rsuffix="_har")
    s = joined.apply(
        lambda x: (x.site_id_cd in x.site_id_har) and (x.version_cd in x.version_har),
        axis=1,
    )
    assert all(s)


def test_bigger_radius_yields_fewer_clusters(get_dataframe):
    """
    Test whether bigger radius yields fewer clusters per subscriber
    """
    radius = [1, 2, 5, 10, 50]
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")

    h = HartiganCluster(cd, radius[0]).get_dataframe()
    nclusters_small_radius = h.groupby("subscriber").size()

    for r in radius[1:]:
        h = HartiganCluster(cd, r).get_dataframe()
        nclusters_big_radius = h.groupby("subscriber").size()
        assert all(nclusters_small_radius >= nclusters_big_radius)
        nclusters_small_radius = nclusters_big_radius


def test_different_call_days_format(get_dataframe):
    """
    Test whether we can pass different call days format such as table name, SQL query and CallDays class.
    """
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")
    har = HartiganCluster(cd, 50).get_dataframe()
    assert isinstance(har, pd.DataFrame)

    cd.store().result()

    har = HartiganCluster(Table(cd.table_name), 50).get_dataframe()
    assert isinstance(har, pd.DataFrame)

    cd_query = cd.get_query()
    har = HartiganCluster(CustomQuery(cd_query), 50).get_dataframe()
    assert isinstance(har, pd.DataFrame)


def test_call_threshold_works(get_dataframe):
    """
    Test whether a call threshold above 1 limits the number of clusters.
    """
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")

    hartigan = HartiganCluster(cd, 50)
    har_df = hartigan.to_geopandas()
    assert any(har_df.calldays == 1)
    har_df_higher_call_threshold = get_dataframe(
        HartiganCluster(cd, 50, call_threshold=2)
    )
    assert len(har_df) > len(har_df_higher_call_threshold)


def test_buffered_hartigan(get_dataframe):
    """
    Test whether Hartigan produces buffered clusters when buffer is larger than 0.
    """
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")

    har = HartiganCluster(cd, 50, buffer=2).to_geopandas()
    areas = har.geometry.area
    # since the mock data does not have geom_area in the site table we either
    # get the clusters with area equivalent to 2km2 (areas below are in degrees) or None.
    min_area = areas.min()
    max_area = areas.max()
    assert min_area == pytest.approx(0.0011327683603873115)
    assert max_area == pytest.approx(0.001166624454009738)


def test_all_options_hartigan(get_dataframe):
    """
    Test whether Hartigan works when changing all options.
    """
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")

    har = HartiganCluster(cd, 50, buffer=2, call_threshold=2).to_geopandas()
    assert set(har.columns) == set(
        ["subscriber", "geometry", "rank", "calldays", "site_id", "version", "centroid"]
    )


def test_join_returns_the_same_clusters(get_dataframe):
    """
    Test whether joining to another table for which the start and stop time are the same yields the same clusters.
    """
    cd = CallDays("2016-01-01", "2016-01-04", level="versioned-site")

    hartigan = HartiganCluster(cd, 50)
    har_df = hartigan.to_geopandas()
    es = EventScore(start="2016-01-01", stop="2016-01-04", level="versioned-site")

    joined = (
        hartigan.join_to_cluster_components(es)
        .to_geopandas()
        .sort_values(["subscriber", "rank", "calldays"])
    )
    joined.reset_index(inplace=True, drop=True)

    har_df.sort_values(["subscriber", "rank", "calldays"], inplace=True)
    har_df.reset_index(inplace=True, drop=True)

    cols = ["subscriber", "geometry", "rank", "calldays"]
    compare = joined[cols] == har_df[cols]
    assert all(compare.all())


def test_unlisted_methods_raises_error():
    """
    Test whether unlisted methods raise error
    """
    with pytest.raises(ValueError):
        subscriber_location_cluster(
            method="not_listed", start="2016-01-01", stop="2016-01-04"
        )


def test_lack_of_radius_with_hartigan_raises_error():
    """
    Test whether not passing a radius raises when choosing `hartigan` as a method raises an error
    """
    with pytest.raises(ValueError):
        subscriber_location_cluster(
            method="hartigan", start="2016-01-01", stop="2016-01-04"
        )


def test_subscriber_location_clusters_defaults():
    """Test that minimal call to subscriber_location_cluster creates expected object."""
    clus = subscriber_location_cluster(
        method="hartigan", start="2016-01-01", stop="2016-01-04", radius=1
    )
    assert 0 == clus.buffer
    assert 0 == clus.call_threshold
