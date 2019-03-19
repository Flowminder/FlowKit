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
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.core.spatial_unit import (
    LatLonSpatialUnit,
    VersionedCellSpatialUnit,
    VersionedSiteSpatialUnit,
    AdminSpatialUnit,
    GridSpatialUnit,
)
from flowmachine.features import daily_location, Flows
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


def test_valid_geojson():
    """
    Check that valid geojson is returned.

    """
    test_geojson = [
        daily_location("2016-01-01", "2016-01-02").aggregate(),
        daily_location(
            "2016-01-01", "2016-01-02", spatial_unit=GridSpatialUnit(size=100)
        ).aggregate(),
        daily_location(
            "2016-01-01", "2016-01-02", spatial_unit=LatLonSpatialUnit()
        ).aggregate(),
        daily_location(
            "2016-01-01", "2016-01-02", spatial_unit=VersionedSiteSpatialUnit()
        ).aggregate(),
        daily_location(
            "2016-01-01", "2016-01-02", spatial_unit=VersionedCellSpatialUnit()
        ).aggregate(),
        daily_location(
            "2016-01-01", "2016-01-02", spatial_unit=AdminSpatialUnit(level=2)
        ).aggregate(),
        daily_location(
            "2016-01-01",
            "2016-01-02",
            spatial_unit=AdminSpatialUnit(level=2, column_name="admin2name"),
        ).aggregate(),
    ]
    for o in test_geojson:
        assert geojson.loads(o.to_geojson_string()).is_valid


def test_correct_geojson():
    """
    Check that the geojson actually contains the right features. 
    """
    js = (
        daily_location(
            "2016-01-01", "2016-01-02", spatial_unit=AdminSpatialUnit(level=2)
        )
        .aggregate()
        .to_geojson()
    )
    pcod = "524 2 05"
    count = 25
    found = False
    for feature in js["features"]:
        if (feature["properties"]["pcod"] == pcod) and (
            feature["properties"]["total"] == count
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
        "2016-01-01", "2016-01-02", spatial_unit=AdminSpatialUnit(level=2)
    ).aggregate().to_geojson_file(js_file)
    with open(js_file) as fin:
        js = json.load(fin)
    os.remove(js_file)
    pcod = "524 2 05"
    count = 25
    found = False
    for feature in js["features"]:
        if (feature["properties"]["pcod"] == pcod) and (
            feature["properties"]["total"] == count
        ):
            found = True
            break
    assert found


def test_flows_geojson(get_dataframe):
    """
    Test geojson works for flows with non-standard column names.
    """

    dl = daily_location(
        "2016-01-01", spatial_unit=AdminSpatialUnit(level=2, column_name="admin2name")
    )
    dl2 = daily_location(
        "2016-01-02", spatial_unit=AdminSpatialUnit(level=2, column_name="admin2name")
    )
    fl = Flows(dl, dl2)
    js = fl.to_geojson()
    df = get_dataframe(fl)
    check_features = [js["features"][0], js["features"][5], js["features"][7]]
    for feature in check_features:
        outflows = feature["properties"]["outflows"]
        df_src = df[
            df.admin2name_from == feature["properties"]["admin2name"]
        ].set_index("admin2name_to")
        for dest, tot in outflows.items():
            assert tot == df_src.loc[dest]["count"]


def test_reprojection():
    """
    Test that in db reprojection works.

    """
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=LatLonSpatialUnit()
    ).aggregate()
    js = dl.to_geojson(crs=2770)  # OSGB36
    assert js["features"][0]["geometry"]["coordinates"] == [
        -8094697.51781301,
        9465052.88370377,
    ]
    assert js["properties"]["crs"] == proj4string(dl.connection, 2770)


def test_geojson_cache():
    """
    Test geojson is cached locally.
    """
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=LatLonSpatialUnit()
    ).aggregate()
    js = dl.to_geojson(crs=2770)  # OSGB36
    assert js == dl._geojson[proj4string(dl.connection, 2770)]


def test_geojson_cache_exluded_from_pickle():
    """Test that cached geojson is not going to get pickled."""
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=LatLonSpatialUnit()
    ).aggregate()
    js = dl.to_geojson(crs=2770)  # OSGB36
    assert "_geojson" not in dl.__getstate__()  # Check excluded from pickle


def test_geojson_caching_off():
    """Test that switching off caching clears the cache, and doesn't add to it."""
    dl = daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=LatLonSpatialUnit()
    ).aggregate()
    js = dl.to_geojson(crs=2770)  # OSGB36
    dl.turn_off_caching()  # Check caching for geojson switches off
    with pytest.raises(KeyError):
        dl._geojson[proj4string(dl.connection, 2770)]
    js = dl.to_geojson(crs=2770)  # OSGB36
    with pytest.raises(KeyError):
        dl._geojson[proj4string(dl.connection, 2770)]
