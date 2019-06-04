# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import CustomQuery
from flowmachine.core.spatial_unit import (
    BaseSpatialUnit,
    CellSpatialUnit,
    LatLonSpatialUnit,
    VersionedCellSpatialUnit,
    VersionedSiteSpatialUnit,
    PolygonSpatialUnit,
    admin_spatial_unit,
    grid_spatial_unit,
)
import pytest


def test_spatial_unit_column_names(exemplar_spatial_unit_param):
    """
    Test that the SpatialUnit classes have accurate column_names properties.
    """
    if CellSpatialUnit() == exemplar_spatial_unit_param:
        pytest.skip(
            "CellSpatialUnit does not have a column_names property (not a Query)"
        )
    su = exemplar_spatial_unit_param
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
        (admin_spatial_unit, {"level": 3}, ["pcod"]),
        (admin_spatial_unit, {"level": 3, "column_name": "admin3pcod"}, ["pcod"]),
        (admin_spatial_unit, {"level": 3, "column_name": "admin3name"}, ["admin3name"]),
        (grid_spatial_unit, {"size": 5}, ["grid_id"]),
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
        (admin_spatial_unit, {"level": 2}),
        (admin_spatial_unit, {"level": 2, "column_name": "admin2name"}),
        (VersionedSiteSpatialUnit, {}),
        (VersionedCellSpatialUnit, {}),
        (CellSpatialUnit, {}),
        (LatLonSpatialUnit, {}),
        (grid_spatial_unit, {"size": 5}),
        (
            PolygonSpatialUnit,
            {"polygon_column_names": "admin3pcod", "polygon_table": "geography.admin3"},
        ),
        (
            PolygonSpatialUnit,
            {
                "polygon_column_names": "id",
                "polygon_table": "infrastructure.sites",
                "geom_col": "geom_point",
            },
        ),
    ],
)
def test_spatial_unit_equals_itself(spatial_unit, kwargs):
    """
    Test that instances of the SpatialUnit classes are equal to themselves.
    """
    su1 = spatial_unit(**kwargs)
    su2 = spatial_unit(**kwargs)
    assert su1 == su2
    assert hash(su1) == hash(su2)


def test_cell_spatial_unit_not_equal_to_other_spatial_unit():
    """
    Test that a CellSpatialUnit is not equal to a VersionedCellSpatialUnit.
    """
    su1 = CellSpatialUnit()
    su2 = VersionedCellSpatialUnit()
    assert su1 != su2
    assert su2 != su1


def test_different_spatial_units_are_not_equal():
    """
    Test that two different spatial units are not equal.
    """
    su1 = VersionedCellSpatialUnit()
    su2 = VersionedSiteSpatialUnit()
    assert su1 != su2


def test_different_level_admin_spatial_units_are_not_equal():
    """
    Test that two admin spatial units with different levels are not equal.
    """
    su1 = admin_spatial_unit(level=1)
    su2 = admin_spatial_unit(level=3)
    assert su1 != su2


def test_different_column_name_admin_spatial_units_are_not_equal():
    """
    Test that two admin spatial units with different column_names are not equal.
    """
    su1 = admin_spatial_unit(level=3, column_name="admin3pcod")
    su2 = admin_spatial_unit(level=3, column_name="admin3name")
    assert su1 != su2


def test_different_grid_spatial_units_are_not_equal():
    """
    Test that two grid spatial units with different sizes are not equal.
    """
    su1 = grid_spatial_unit(size=5)
    su2 = grid_spatial_unit(size=50)
    assert su1 != su2
