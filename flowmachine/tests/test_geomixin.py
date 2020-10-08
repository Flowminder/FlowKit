# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the GeoDataMixin mixin.
"""
import os
import json
from typing import List

import geojson


import pytest

from flowmachine.core import Query
from flowmachine.core.context import get_db
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.core import make_spatial_unit
from flowmachine.features import daily_location
from flowmachine.utils import proj4string


def test_massive_geojson():
    """
    Check that geomixin methods are robust to queries with a large number of
    rows.
    """

    class ManyRows(GeoDataMixin, Query):
        """
        Query which generates 2,685 rows of garbage, where each row has
        a 'junk_data' field containing a 100,000 byte bytea.
        i.e. a query which will be at least 268,500,000 bytes in size.

        References
        ----------

        https://dba.stackexchange.com/a/22571
        """

        def __init__(self):
            super().__init__()

        def _make_query(self):
            pass

        @property
        def column_names(self) -> List[str]:
            return ["gid", "id", "geom", "junk_data"]

        def _geo_augmented_query(self):
            return (
                """SELECT 
                  n as gid, random() AS id, ST_POINT(0, 0) as geom,
                  junk_data
                FROM  
                   generate_series (1,2685) AS x(n)
                   LEFT JOIN (
                   SELECT decode(
                            string_agg(
                             lpad(
                              to_hex(
                               width_bucket(random(), 0, 1, 256)-1),2,'0') ,''), 'hex')::bytea as junk_data
                    FROM generate_series(1, 100000)) d ON true""",
                ["gid", "id", "geom"],
            )

    ManyRows().to_geojson()  # This will error if the geojson couldn't be constructed


def test_valid_geojson(exemplar_spatial_unit_param):
    """
    Check that valid geojson is returned.

    """
    if not exemplar_spatial_unit_param.has_geography:
        pytest.skip("Query with spatial_unit=CellSpatialUnit() has no geometry.")
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=exemplar_spatial_unit_param
    ).aggregate()
    assert geojson.loads(dl.to_geojson_string()).is_valid


def test_geo_augmented_query_raises_error():
    """
    Test that a ValueError is raised when attempting to get geojson for a query
    with no geography data.
    """
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("cell")
    ).aggregate()
    with pytest.raises(ValueError):
        dl.to_geojson_string()


def test_correct_geojson():
    """
    Check that the geojson actually contains the right features.
    """
    js = (
        daily_location(
            "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("admin", level=2)
        )
        .aggregate()
        .to_geojson()
    )
    pcod = "524 2 05"
    count = 25
    found = False
    for feature in js["features"]:
        if (feature["properties"]["pcod"] == pcod) and (
            feature["properties"]["value"] == count
        ):
            found = True
            break
    assert found


def test_geojson_file_output(tmpdir):
    """
    Test that geojson can be successfully written to a file.

    """

    js_file = tmpdir / "geojson_test.json"

    daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("admin", level=2)
    ).aggregate().to_geojson_file(js_file)
    with open(js_file) as fin:
        js = json.load(fin)
    os.remove(js_file)
    pcod = "524 2 05"
    count = 25
    found = False
    for feature in js["features"]:
        if (feature["properties"]["pcod"] == pcod) and (
            feature["properties"]["value"] == count
        ):
            found = True
            break
    assert found


def test_reprojection():
    """
    Test that in db reprojection works.

    """
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
    ).aggregate()
    js = dl.to_geojson(crs=2770)  # OSGB36
    assert js["features"][0]["geometry"]["coordinates"] == pytest.approx(
        [-8094697.52, 9465052.88]
    )
    assert js["properties"]["crs"] == proj4string(get_db(), 2770)


def test_geojson_cache():
    """
    Test geojson is cached locally.
    """
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
    ).aggregate()
    js = dl.to_geojson(crs=2770)  # OSGB36
    assert js == dl._geojson[proj4string(get_db(), 2770)]


def test_geojson_cache_exluded_from_pickle():
    """Test that cached geojson is not going to get pickled."""
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
    ).aggregate()
    js = dl.to_geojson(crs=2770)  # OSGB36
    assert "_geojson" not in dl.__getstate__()  # Check excluded from pickle


def test_geojson_caching_off():
    """Test that switching off caching clears the cache, and doesn't add to it."""
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
    ).aggregate()
    js = dl.to_geojson(crs=2770)  # OSGB36
    dl.turn_off_caching()  # Check caching for geojson switches off
    with pytest.raises(KeyError):
        dl._geojson[proj4string(get_db(), 2770)]
    js = dl.to_geojson(crs=2770)  # OSGB36
    with pytest.raises(KeyError):
        dl._geojson[proj4string(get_db(), 2770)]
