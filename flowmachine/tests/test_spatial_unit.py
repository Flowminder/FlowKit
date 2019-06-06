# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import CustomQuery
from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.core.spatial_unit import *
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


def test_get_geom_query_column_names(
    exemplar_spatial_unit_param, get_column_names_from_run
):
    """
    Test that the get_geom_query method returns a query with the correct columns.
    """
    if not exemplar_spatial_unit_param.has_geography:
        pytest.skip("CellSpatialUnit does not have a get_geom_query method")
    geom_query = exemplar_spatial_unit_param.get_geom_query()
    cols = exemplar_spatial_unit_param.location_columns + ["geom"]
    cq = CustomQuery(geom_query, cols)
    assert sorted(get_column_names_from_run(cq)) == sorted(cols)


@pytest.mark.parametrize(
    "make_spatial_unit_args, loc_cols",
    [
        ({"spatial_unit_type": "lat-lon"}, ["lat", "lon"]),
        (
            {"spatial_unit_type": "versioned-cell"},
            ["location_id", "version", "lon", "lat"],
        ),
        ({"spatial_unit_type": "versioned-site"}, ["site_id", "version", "lon", "lat"]),
        (
            {
                "spatial_unit_type": "polygon",
                "region_id_column_name": "id",
                "polygon_table": "infrastructure.sites",
                "geom_column": "geom_point",
            },
            ["id"],
        ),
        (
            {
                "spatial_unit_type": "polygon",
                "region_id_column_name": ["id"],
                "polygon_table": "infrastructure.sites",
                "geom_column": "geom_point",
            },
            ["id"],
        ),
        ({"spatial_unit_type": "admin", "level": 3}, ["pcod"]),
        (
            {
                "spatial_unit_type": "admin",
                "level": 3,
                "region_id_column_name": "admin3pcod",
            },
            ["pcod"],
        ),
        (
            {
                "spatial_unit_type": "admin",
                "level": 3,
                "region_id_column_name": "admin3name",
            },
            ["admin3name"],
        ),
        ({"spatial_unit_type": "grid", "size": 5}, ["grid_id"]),
    ],
)
def test_spatial_unit_location_columns(make_spatial_unit_args, loc_cols):
    """
    Test that the SpatialUnit classes have the correct location_columns properties.
    """
    su = make_spatial_unit(**make_spatial_unit_args)
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
        geom_column="geom_point",
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


@pytest.mark.parametrize(
    "make_spatial_unit_args",
    [
        {"spatial_unit_type": "admin", "level": 2},
        {
            "spatial_unit_type": "admin",
            "level": 2,
            "region_id_column_name": "admin2name",
        },
        {"spatial_unit_type": "versioned-site"},
        {"spatial_unit_type": "versioned-cell"},
        {"spatial_unit_type": "cell"},
        {"spatial_unit_type": "lat-lon"},
        {"spatial_unit_type": "grid", "size": 5},
        {
            "spatial_unit_type": "polygon",
            "region_id_column_name": "admin3pcod",
            "polygon_table": "geography.admin3",
        },
        {
            "spatial_unit_type": "polygon",
            "region_id_column_name": "id",
            "polygon_table": "infrastructure.sites",
            "geom_column": "geom_point",
        },
    ],
)
def test_spatial_unit_equals_itself(make_spatial_unit_args):
    """
    Test that instances of the SpatialUnit classes are equal to themselves.
    """
    # Can't use exemplar_spatial_unit_param here because we need to create two
    # different but equal spatial units.
    su1 = make_spatial_unit(**make_spatial_unit_args)
    su2 = make_spatial_unit(**make_spatial_unit_args)
    assert su1 == su2
    assert hash(su1) == hash(su2)


def test_cell_spatial_unit_not_equal_to_other_spatial_unit():
    """
    Test that a CellSpatialUnit is not equal to a versioned_cell_spatial_unit.
    """
    su1 = CellSpatialUnit()
    su2 = versioned_cell_spatial_unit()
    assert su1 != su2
    assert su2 != su1


def test_different_spatial_units_are_not_equal():
    """
    Test that two different spatial units are not equal.
    """
    su1 = versioned_cell_spatial_unit()
    su2 = versioned_site_spatial_unit()
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
    su1 = admin_spatial_unit(level=3, region_id_column_name="admin3pcod")
    su2 = admin_spatial_unit(level=3, region_id_column_name="admin3name")
    assert su1 != su2


def test_different_grid_spatial_units_are_not_equal():
    """
    Test that two grid spatial units with different sizes are not equal.
    """
    su1 = grid_spatial_unit(size=5)
    su2 = grid_spatial_unit(size=50)
    assert su1 != su2


@pytest.mark.parametrize(
    "make_spatial_unit_args",
    [
        {"spatial_unit_type": "INVALID_SPATIAL_UNIT_TYPE"},
        {"spatial_unit_type": "admin"},
        {"spatial_unit_type": "grid"},
        {"spatial_unit_type": "polygon", "polygon_table": "geography.admin3"},
        {"spatial_unit_type": "polygon", "region_id_column_name": "DUMMY_COLUMN_NAME"},
    ],
)
def test_make_spatial_unit_raises_errors(make_spatial_unit_args):
    """
    Test that make_spatial_unit raises a ValueError when bad arguments are passed.
    """
    with pytest.raises(ValueError):
        su = make_spatial_unit(**make_spatial_unit_args)


@pytest.mark.parametrize(
    "make_spatial_unit_args, criterion, negate",
    [
        ({"spatial_unit_type": "cell"}, "has_geography", False),
        ({"spatial_unit_type": "versioned-cell"}, "has_geography", True),
        ({"spatial_unit_type": "admin", "level": 3}, "has_lat_lon_columns", False),
        ({"spatial_unit_type": "lat-lon"}, "has_lat_lon_columns", True),
        ({"spatial_unit_type": "admin", "level": 3}, "is_network_object", False),
        ({"spatial_unit_type": "cell"}, "is_network_object", True),
        ({"spatial_unit_type": "versioned-site"}, "is_network_object", True),
        ({"spatial_unit_type": "lat-lon"}, "is_polygon", False),
        ({"spatial_unit_type": "grid", "size": 10}, "is_polygon", True),
    ],
)
def test_verify_criterion(make_spatial_unit_args, criterion, negate):
    """
    Test that the verify_criterion method raises an InvalidSpatialUnitError
    when the criterion is not met.
    """
    su = make_spatial_unit(**make_spatial_unit_args)
    with pytest.raises(InvalidSpatialUnitError):
        su.verify_criterion(criterion, negate=negate)


def test_verify_criterion_raises_value_error():
    """
    Test that the verify_criterion method raises a ValueError if the criterion
    is not recognised.
    """
    su = CellSpatialUnit()
    with pytest.raises(ValueError):
        su.verify_criterion("BAD_CRITERION")
