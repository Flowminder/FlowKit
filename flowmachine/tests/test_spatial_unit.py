# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import CustomQuery
from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.core.spatial_unit import *
import pytest


def test_spatial_unit_column_names(exemplar_spatial_unit_param):
    """
    Test that the *SpatialUnit classes have accurate column_names properties.
    """
    if isinstance(exemplar_spatial_unit_param, CellSpatialUnit):
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
    cols = exemplar_spatial_unit_param.location_id_columns + ["geom"]
    cq = CustomQuery(geom_query, cols)
    assert sorted(get_column_names_from_run(cq)) == sorted(cols)


@pytest.mark.parametrize(
    "make_spatial_unit_args, loc_cols",
    [
        ({"spatial_unit_type": "lon-lat"}, ["lon", "lat"]),
        (
            {"spatial_unit_type": "versioned-cell"},
            ["location_id", "version", "lon", "lat"],
        ),
        ({"spatial_unit_type": "versioned-site"}, ["site_id", "version", "lon", "lat"]),
        (
            {
                "spatial_unit_type": "polygon",
                "region_id_column_name": "id",
                "geom_table": "infrastructure.sites",
                "geom_column": "geom_point",
            },
            ["id"],
        ),
        (
            {
                "spatial_unit_type": "polygon",
                "region_id_column_name": ["id"],
                "geom_table": "infrastructure.sites",
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
def test_spatial_unit_location_id_columns(make_spatial_unit_args, loc_cols):
    """
    Test that the *SpatialUnit classes have the correct location_id_columns properties.
    """
    su = make_spatial_unit(**make_spatial_unit_args)
    assert loc_cols == su.location_id_columns


def test_polygon_spatial_unit_column_list():
    """
    Test that, when supplying polygon_column_names to PolygonSpatialUnit as a
    list, location_id_columns returns it as a new list.
    """
    passed_cols = ["id"]
    psu = PolygonSpatialUnit(
        geom_table_column_names=passed_cols,
        geom_table="infrastructure.sites",
        geom_column="geom_point",
    )
    loc_cols = psu.location_id_columns
    assert passed_cols == loc_cols
    assert id(passed_cols) != id(loc_cols)


def test_missing_location_columns_raises_error():
    """
    Test that a ValueError is raised if the location_id_column_names passed to
    GeomSpatialUnit are not a subset of column_names.
    """
    with pytest.raises(ValueError, match="['NOT_A_COLUMN']"):
        su = LonLatSpatialUnit(location_id_column_names=["location_id", "NOT_A_COLUMN"])


@pytest.mark.parametrize(
    "make_spatial_unit_args, expected_name",
    [
        ({"spatial_unit_type": "admin", "level": 2}, "admin2"),
        ({"spatial_unit_type": "versioned-site"}, "versioned-site"),
        ({"spatial_unit_type": "versioned-cell"}, "versioned-cell"),
        ({"spatial_unit_type": "cell"}, "cell"),
        ({"spatial_unit_type": "lon-lat"}, "lon-lat"),
        ({"spatial_unit_type": "grid", "size": 5}, "grid"),
        (
            {
                "spatial_unit_type": "polygon",
                "region_id_column_name": "id",
                "geom_table": "infrastructure.sites",
                "geom_column": "geom_point",
            },
            "polygon",
        ),
    ],
)
def test_canonical_names(make_spatial_unit_args, expected_name):
    """
    Test that canonical names are correct and present.
    """
    assert make_spatial_unit(**make_spatial_unit_args).canonical_name == expected_name


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
        {"spatial_unit_type": "lon-lat"},
        {"spatial_unit_type": "grid", "size": 5},
        {
            "spatial_unit_type": "polygon",
            "region_id_column_name": "admin3pcod",
            "geom_table": "geography.admin3",
        },
        {
            "spatial_unit_type": "polygon",
            "region_id_column_name": "id",
            "geom_table": "infrastructure.sites",
            "geom_column": "geom_point",
        },
    ],
)
def test_spatial_unit_equals_itself(make_spatial_unit_args):
    """
    Test that instances of the *SpatialUnit classes are equal to themselves.
    """
    # Can't use exemplar_spatial_unit_param here because we need to create two
    # different but equal spatial units.
    su1 = make_spatial_unit(**make_spatial_unit_args)
    su2 = make_spatial_unit(**make_spatial_unit_args)
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


def test_different_level_AdminSpatialUnits_are_not_equal():
    """
    Test that two admin spatial units with different levels are not equal.
    """
    su1 = AdminSpatialUnit(level=1)
    su2 = AdminSpatialUnit(level=3)
    assert su1 != su2


def test_different_column_name_AdminSpatialUnits_are_not_equal():
    """
    Test that two admin spatial units with different column_names are not equal.
    """
    su1 = AdminSpatialUnit(level=3, region_id_column_name="admin3pcod")
    su2 = AdminSpatialUnit(level=3, region_id_column_name="admin3name")
    assert su1 != su2


def test_different_GridSpatialUnits_are_not_equal():
    """
    Test that two grid spatial units with different sizes are not equal.
    """
    su1 = GridSpatialUnit(size=5)
    su2 = GridSpatialUnit(size=50)
    assert su1 != su2


@pytest.mark.parametrize(
    "make_spatial_unit_args",
    [
        {"spatial_unit_type": "INVALID_SPATIAL_UNIT_TYPE"},
        {"spatial_unit_type": "admin"},
        {"spatial_unit_type": "grid"},
        {"spatial_unit_type": "polygon", "geom_table": "geography.admin3"},
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
        ({"spatial_unit_type": "admin", "level": 3}, "has_lon_lat_columns", False),
        ({"spatial_unit_type": "lon-lat"}, "has_lon_lat_columns", True),
        ({"spatial_unit_type": "admin", "level": 3}, "is_network_object", False),
        ({"spatial_unit_type": "cell"}, "is_network_object", True),
        ({"spatial_unit_type": "versioned-site"}, "is_network_object", True),
        ({"spatial_unit_type": "lon-lat"}, "is_polygon", False),
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


@pytest.mark.parametrize("spatial_unit_type", ["cell", "lon-lat"])
@pytest.mark.parametrize(
    "locations", [{"BAD_COLUMN": "DUMMY_VALUE"}, [{"BAD_COLUMN": "DUMMY_VALUE"}]]
)
def test_location_subset_clause_raises_error(spatial_unit_type, locations):
    """
    Test that the location_subset_clause method raises a ValueError if
    incorrect columns are passed in a dict.
    """
    with pytest.raises(ValueError):
        make_spatial_unit(spatial_unit_type).location_subset_clause(locations)


@pytest.mark.parametrize(
    "spatial_unit_type, locations, expected",
    [
        ("cell", "loc", "WHERE location_id = 'loc'"),
        ("cell", ["loc1", "loc2"], "WHERE location_id IN ('loc1', 'loc2')"),
        ("cell", {"location_id": "loc"}, "WHERE location_id = 'loc'"),
        (
            "cell",
            [{"location_id": "loc1"}, {"location_id": "loc2"}],
            "WHERE (location_id = 'loc1') OR (location_id = 'loc2')",
        ),
        (
            "versioned-site",
            {"site_id": "loc", "version": "v"},
            "WHERE site_id = 'loc' AND version = 'v'",
        ),
        (
            "versioned-site",
            [
                {"site_id": "loc1", "version": "v1"},
                {"site_id": "loc2", "version": "v2"},
            ],
            "WHERE (site_id = 'loc1' AND version = 'v1') OR (site_id = 'loc2' AND version = 'v2')",
        ),
        ("versioned-site", "loc", "WHERE site_id = 'loc'"),
        ("versioned-site", ["loc1", "loc2"], "WHERE site_id IN ('loc1', 'loc2')"),
        (
            "versioned-site",
            [("lon1", "lat1")],
            "WHERE ST_SetSRID(ST_Point(lon, lat), 4326) = 'ST_SetSRID(ST_Point(lon1, lat1), 4326)'",
        ),
        (
            "versioned-site",
            [("lon1", "lat1"), ("lon2", "lat2")],
            "WHERE ST_SetSRID(ST_Point(lon, lat), 4326) IN ('ST_SetSRID(ST_Point(lon1, lat1), 4326)', 'ST_SetSRID(ST_Point(lon2, lat2), 4326)')",
        ),
        (
            "versioned-site",
            {"lon": "lon1", "lat": "lat1"},
            "WHERE ST_SetSRID(ST_Point(lon, lat), 4326) = 'ST_SetSRID(ST_Point(lon1, lat1), 4326)'",
        ),
        (
            "versioned-site",
            [
                {"site_id": "site1", "lon": "lon1", "lat": "lat1"},
                {"site_id": "site2", "lon": "lon2", "lat": "lat2"},
            ],
            "WHERE (site_id = 'site1' AND ST_SetSRID(ST_Point(lon, lat), 4326) = 'ST_SetSRID(ST_Point(lon1, lat1), 4326)') OR (site_id = 'site2' AND ST_SetSRID(ST_Point(lon, lat), 4326) = 'ST_SetSRID(ST_Point(lon2, lat2), 4326)')",
        ),
    ],
)
def test_location_subset_clause_return_value(spatial_unit_type, locations, expected):
    """
    Test that location_subset_clause returns some correct strings.
    """
    su = make_spatial_unit(spatial_unit_type)
    assert expected == su.location_subset_clause(locations)


def test_admin_mapping_table():
    sp_unit = make_spatial_unit(
        "admin",
        level=3,
        mapping_table=CustomQuery(
            "SELECT id, '524 2 05 24' as admin3pcod from infrastructure.cells",
            column_names=["id", "admin3pcod"],
        ),
    )

    assert sp_unit._loc_on == "id"
    assert sp_unit._geom_on == "admin3pcod"
