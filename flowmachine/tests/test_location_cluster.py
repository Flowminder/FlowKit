# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the LocationCluster() class.
"""


from flowmachine.features import LocationCluster


def test_kmeans_returns_correct_results(get_dataframe):
    """
    LocationCluster(method='kmeans') returns correct result.
    """
    l = LocationCluster(method="kmeans", number_of_clusters=2)

    result = get_dataframe(l)
    assert result["cluster_id"].nunique() == 2
    assert result["location_id"].nunique() == 30


def test_dbscan_returns_correct_results(get_dataframe):
    """
    LocationCluster(method='dbscan') returns correct result.
    """
    l = LocationCluster(method="dbscan", distance_tolerance=100, density_tolerance=2)

    result = get_dataframe(l)
    assert result["cluster_id"].nunique() == 3
    assert result["location_id"].nunique() == 29


def test_area_returns_correct_results(get_dataframe):
    """
    LocationCluster(method='area') returns correct result.
    """
    l = LocationCluster(method="area", distance_tolerance=100)

    result = get_dataframe(l)
    assert result["cluster_id"].nunique() == 4
    assert result["location_id"].nunique() == 30


def test_aggregate_returns_correct_results(get_dataframe):
    """
    LocationCluster(aggregate=True) returns correct result.
    """
    l1 = LocationCluster(method="area", distance_tolerance=100)
    l2 = LocationCluster(method="area", distance_tolerance=100, aggregate=True)

    result1 = get_dataframe(l1)
    result2 = get_dataframe(l2)

    assert result1["cluster_id"].nunique() == len(result2)
