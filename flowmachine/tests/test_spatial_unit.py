# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import CustomQuery
from flowmachine.core.spatial_unit import (
    BaseSpatialUnit,
    LatLonSpatialUnit,
    VersionedCellSpatialUnit,
    VersionedSiteSpatialUnit,
    PolygonSpatialUnit,
    AdminSpatialUnit,
    GridSpatialUnit,
)
import pytest


@pytest.mark.parametrize(
    "spatial_unit, kwargs",
    [
        (LatLonSpatialUnit, {}),
        (VersionedCellSpatialUnit, {}),
        (VersionedSiteSpatialUnit, {}),
        (
            PolygonSpatialUnit,
            {"polygon_column_names": "admin3name", "polygon_table": "geography.admin3"},
        ),
        (
            PolygonSpatialUnit,
            {
                "polygon_column_names": "id",
                "polygon_table": "infrastructure.sites",
                "geom_col": "geom_point",
            },
        ),
        (AdminSpatialUnit, {"level": 3}),
        (AdminSpatialUnit, {"level": 3, "column_name": "admin3name"}),
        (GridSpatialUnit, {"size": 5}),
    ],
)
def test_spatial_unit_column_names(spatial_unit, kwargs):
    """
    Test that the SpatialUnit classes have accurate column_names properties.
    """
    su = spatial_unit(**kwargs)
    assert su.head(0).columns.tolist() == su.column_names


@pytest.mark.parametrize(
    "spatial_unit, kwargs, loc_cols",
    [
        (LatLonSpatialUnit, {}, ["lat", "lon"]),
        (VersionedCellSpatialUnit, {}, ["location_id", "version", "lon", "lat"]),
        (VersionedSiteSpatialUnit, {}, ["site_id", "version", "lon", "lat"]),
        (
            PolygonSpatialUnit,
            {
                "polygon_column_names": "id",
                "polygon_table": "infrastructure.sites",
                "geom_col": "geom_point",
            },
            ["id"],
        ),
        (
            PolygonSpatialUnit,
            {
                "polygon_column_names": ["id"],
                "polygon_table": "infrastructure.sites",
                "geom_col": "geom_point",
            },
            ["id"],
        ),
        (AdminSpatialUnit, {"level": 3}, ["pcod"]),
        (AdminSpatialUnit, {"level": 3, "column_name": "admin3pcod"}, ["pcod"]),
        (AdminSpatialUnit, {"level": 3, "column_name": "admin3name"}, ["admin3name"]),
        (GridSpatialUnit, {"size": 5}, ["grid_id"]),
    ],
)
def test_spatial_unit_location_columns(spatial_unit, kwargs, loc_cols):
    """
    Test that the SpatialUnit classes have the correct location_columns properties.
    """
    su = spatial_unit(**kwargs)
    assert loc_cols == su.location_columns


def test_polygon_spatial_unit_column_list():
    """
    Test that, when supplying polygon_column_names to PolygonSpatialUnit as a
    list, location_columns returns it as a new list.
    """
    passed_cols = ["id"]
    psu = PolygonSpatialUnit(
        polygon_column_names=passed_cols,
        polygon_table="infrastructure.sites",
        geom_col="geom_point",
    )
    loc_cols = psu.location_columns
    assert passed_cols == loc_cols
    assert id(passed_cols) != id(loc_cols)


def test_missing_location_columns_raises_error():
    """
    Test that a ValueError is raised if the location_column_names passed to
    SpatialUnit are not a subset of column_names.
    """

    class TestSpatialUnit(BaseSpatialUnit):
        def geo_augment(self, query):
            pass

    with pytest.raises(ValueError, match="['NOT_A_COLUMN']"):
        su = TestSpatialUnit(
            selected_column_names=[
                "id AS location_id",
                "date_of_first_service",
                "date_of_last_service",
            ],
            location_column_names=["location_id", "NOT_A_COLUMN"],
        )


@pytest.mark.parametrize(
    "spatial_unit, kwargs",
    [
        (LatLonSpatialUnit, {}),
        (VersionedCellSpatialUnit, {}),
        (VersionedSiteSpatialUnit, {}),
        (
            PolygonSpatialUnit,
            {"polygon_column_names": "admin3pcod", "polygon_table": "geography.admin3"},
        ),
        (AdminSpatialUnit, {"level": 2}),
        (AdminSpatialUnit, {"level": 2, "column_name": "admin2name"}),
        (GridSpatialUnit, {"size": 5}),
    ],
)
def test_geo_augment_columns(spatial_unit, kwargs):
    """
    Test that the columns returned by the geo_augment method are correct.
    """
    su = spatial_unit(**kwargs)
    sql, cols = su.geo_augment(su)
    cq = CustomQuery(sql, cols)
    assert cq.head(0).columns.tolist() == cols


@pytest.mark.parametrize(
    "spatial_unit", [VersionedCellSpatialUnit, VersionedSiteSpatialUnit]
)
@pytest.mark.parametrize("return_geometry", [True, False])
def test_distance_matrix_columns(spatial_unit, return_geometry):
    """
    Test that the columns returned by the distance_matrix_columns method match
    the columns of the distance_matrix_query.
    """
    su = spatial_unit()
    sql = su.distance_matrix_query(return_geometry=return_geometry)
    cols = su.distance_matrix_columns(return_geometry=return_geometry)
    cq = CustomQuery(sql, cols)
    assert cq.head(0).columns.tolist() == cols
