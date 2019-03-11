# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.spatial_unit import (
    SpatialUnit,
    LatLonSpatialUnit,
    VersionedCellSpatialUnit,
    VersionedSiteSpatialUnit,
    PolygonSpatialUnit,
    AdminSpatialUnit,
    GridSpatialUnit,
)
import pytest


@pytest.mark.parametrize(
    "spatial_unit, args",
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
def test_spatial_unit_column_names(spatial_unit, args):
    """
    Test that the SpatialUnit classes have accurate column_names properties.
    """
    instance = spatial_unit(**args)
    assert instance.head(0).columns.tolist() == instance.column_names


@pytest.mark.parametrize(
    "spatial_unit, args, loc_cols",
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
def test_spatial_unit_location_columns(spatial_unit, args, loc_cols):
    """
    Test that the SpatialUnit classes have the correct location_columns properties.
    """
    instance = spatial_unit(**args)
    assert loc_cols == instance.location_columns


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
    with pytest.raises(ValueError, match="['NOT_A_COLUMN']"):
        su = SpatialUnit(
            selected_column_names=[
                "id AS location_id",
                "date_of_first_service",
                "date_of_last_service",
            ],
            location_column_names=["location_id", "NOT_A_COLUMN"],
        )
