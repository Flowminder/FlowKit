# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the LocationArea() class.
"""
import pytest

from flowmachine.features import LocationArea

n_sites = 30
site_ids = ["o9yyxY", "B8OaG5", "DbWg4K", "0xqNDj"]


def test_radius_returns_record_set(get_dataframe):
    """
    LocationArea('radius') returns N-sized result set.
    """
    result = get_dataframe(LocationArea(method="radius", radius=50))
    assert len(result) == n_sites
    for site in site_ids:
        assert site in result["location_id"].tolist()


def test_voronois_returns_record_set(get_dataframe):
    """
    LocationArea('voronois') returns N-sized result set.
    """
    result = get_dataframe(LocationArea(method="voronois"))
    assert len(result) == n_sites
    for site in site_ids:
        assert site in result["location_id"].tolist()


def test_radius_voronois_returns_record_set(get_dataframe):
    """
    LocationArea('radius-voronois') returns N-sized result set.
    """
    result = get_dataframe(LocationArea(method="radius-voronois", radius=50))
    assert len(result) == n_sites
    for site in site_ids:
        assert site in result["location_id"].tolist()


def test_viewshed_returns_right_results(get_dataframe):
    """
    LocationArea('viewshed') returns right results.
    """
    result = get_dataframe(LocationArea(method="viewshed", radius=0.1))
    assert len(result) == 1
    assert result["location_id"].iloc[0] == "o9yyxY"


def test_radio_propagation_raises_error():
    """
    LocationArea('radio-propagation') raises error.
    """
    with pytest.raises(NotImplementedError):
        LocationArea(method="radio-propagation")


def test_viewshed_fails_if_no_dem_provided():
    """
    LocationArea() raises error if no DEM is passed.
    """
    with pytest.raises(ValueError):
        LocationArea(method="viewshed", dem=None)


def test_unrecognized_method_raises_error():
    """
    LocationArea() raises error when unrecognized method is passed.
    """
    with pytest.raises(ValueError):
        LocationArea(method="foo")
