# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for Subscriber Location Clustering methods. Those methods cluster
location based on different methods, offering good options for
reducing the dimensionality of the problem.
"""
from typing import List

import pandas as pd


import pytest
from geopandas import GeoSeries
from shapely.geometry import box, MultiPoint

from flowmachine.core import Table, CustomQuery, make_spatial_unit
from flowmachine.core.query import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features import (
    CallDays,
    HartiganCluster,
    EventScore,
    SubscriberLocations,
)


@pytest.mark.usefixtures("skip_datecheck")
def test_hartigan_column_names(get_column_names_from_run):
    """Test that Hartigan has correct column_names property."""
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )
    hartigan = HartiganCluster(calldays=cd, radius=50)
    assert get_column_names_from_run(hartigan) == hartigan.column_names


@pytest.mark.usefixtures("skip_datecheck")
def test_joined_hartigan_column_names(get_column_names_from_run):
    """Test that Hartigan has correct column_names property."""
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )
    hartigan = HartiganCluster(calldays=cd, radius=50)
    es = EventScore(
        start="2016-01-01",
        stop="2016-01-05",
        spatial_unit=make_spatial_unit("versioned-site"),
    )
    joined = hartigan.join_to_cluster_components(es)
    assert get_column_names_from_run(joined) == joined.column_names


def test_hartigan_type_error():
    """Test that Hartigan errors if given a non-query like calldays"""
    with pytest.raises(TypeError):
        HartiganCluster(calldays="fudge", radius=50)


@pytest.mark.usefixtures("skip_datecheck")
def test_joined_hartigan_type_error():
    """Test that joining hartigan to something which isn't query like raises a type error."""
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )
    hartigan = HartiganCluster(calldays=cd, radius=50)
    with pytest.raises(TypeError):
        hartigan.join_to_cluster_components("banana")


class Sites(GeoDataMixin, Query):
    """
    Selects the geometric coordinates of the versioned-sites.
    """

    @property
    def column_names(self) -> List[str]:
        return ["site_id", "version", "geom_point"]

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
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )

    hartigan = HartiganCluster(calldays=cd, radius=50)
    har_df = hartigan.to_geopandas()
    sites = Sites().to_geopandas().set_index(["site_id", "version"])
    towers = GeoSeries(har_df.apply(lambda x: get_geom_point(x, sites), 1))
    s = har_df.intersects(towers)
    assert all(s)


def test_first_call_day_in_first_cluster(get_dataframe):
    """
    Test that the first ranked call day of each subscriber is in the first cluster of each subscriber.
    """
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )
    cd_df = get_dataframe(cd)

    hartigan = HartiganCluster(calldays=cd, radius=50)
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
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )

    h = get_dataframe(HartiganCluster(calldays=cd, radius=radius[0]))
    nclusters_small_radius = h.groupby("subscriber").size()

    for r in radius[1:]:
        h = get_dataframe(HartiganCluster(calldays=cd, radius=r))
        nclusters_big_radius = h.groupby("subscriber").size()
        assert all(nclusters_small_radius >= nclusters_big_radius)
        nclusters_small_radius = nclusters_big_radius


def test_different_call_days_format(get_dataframe):
    """
    Test whether we can pass different call days format such as table name, SQL query and CallDays class.
    """
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )
    har = get_dataframe(HartiganCluster(calldays=cd, radius=50))
    assert isinstance(har, pd.DataFrame)

    cd.store().result()

    har = get_dataframe(
        HartiganCluster(
            calldays=Table(cd.fully_qualified_table_name, columns=cd.column_names),
            radius=50,
        )
    )
    assert isinstance(har, pd.DataFrame)

    cd_query = cd.get_query()
    har = get_dataframe(
        HartiganCluster(calldays=CustomQuery(cd_query, cd.column_names), radius=50)
    )
    assert isinstance(har, pd.DataFrame)


def test_call_threshold_works(get_dataframe):
    """
    Test whether a call threshold above 1 limits the number of clusters.
    """
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )

    hartigan = HartiganCluster(calldays=cd, radius=50)
    har_df = hartigan.to_geopandas()
    assert any(har_df.calldays == 1)
    har_df_higher_call_threshold = get_dataframe(
        HartiganCluster(calldays=cd, radius=50, call_threshold=2)
    )
    assert len(har_df) > len(har_df_higher_call_threshold)


def test_buffered_hartigan():
    """
    Test whether Hartigan produces buffered clusters when buffer is larger than 0.
    """
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )

    har = HartiganCluster(calldays=cd, radius=50, buffer=2).to_geopandas()
    areas = har.geometry.area
    # since the mock data does not have geom_area in the site table we either
    # get the clusters with area equivalent to 2km2 (areas below are in degrees) or None.
    min_area = areas.min()
    max_area = areas.max()
    assert min_area == pytest.approx(0.0011327683603873115)
    assert max_area == pytest.approx(0.001166624454009738)


def test_all_options_hartigan():
    """
    Test whether Hartigan works when changing all options.
    """
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )

    har = HartiganCluster(
        calldays=cd, radius=50, buffer=2, call_threshold=2
    ).to_geopandas()
    assert set(har.columns) == set(
        ["subscriber", "geometry", "rank", "calldays", "site_id", "version", "centroid"]
    )


def test_join_returns_the_same_clusters():
    """
    Test whether joining to another table for which the start and stop time are the same yields the same clusters.
    """
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )

    hartigan = HartiganCluster(calldays=cd, radius=50)
    har_df = hartigan.to_geopandas()
    es = EventScore(
        start="2016-01-01",
        stop="2016-01-04",
        spatial_unit=make_spatial_unit("versioned-site"),
    )

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


def test_hartigan_cluster_bad_calldays_column_names_raises_error():
    """
    Test that using calldays without 'site_id' and 'version' columns raises an error.
    """
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("lon-lat")
        )
    )
    with pytest.raises(ValueError):
        HartiganCluster(calldays=cd, radius=50)


def test_joined_hartigan_cluster_bad_query_column_names_raises_error():
    """
    Test that joining a HartiganCluster to a query without 'site_id' and 'version' columns raises an error.
    """
    cd = CallDays(
        SubscriberLocations(
            "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("versioned-site")
        )
    )
    hartigan = HartiganCluster(calldays=cd, radius=50)
    es = EventScore(
        start="2016-01-01", stop="2016-01-04", spatial_unit=make_spatial_unit("lon-lat")
    )
    with pytest.raises(ValueError):
        hartigan.join_to_cluster_components(es)
