# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from datetime import datetime, timezone

import numpy as np
from flowmachine_core.utility_queries.custom_query import CustomQuery

from flowmachine_core.utility_queries.join_to_location import (
    JoinToLocation,
    location_joined_query,
)
from flowmachine_core.query_bases.spatial_unit import make_spatial_unit
from flowmachine_core.core.errors import InvalidSpatialUnitError
from flowmachine_core.utility_queries.event_table_subset import EventTableSubset


@pytest.fixture
def location_joinable_query():
    return CustomQuery(
        "SELECT '2016-01-01 22:38:06'::timestamptz as datetime, 'JxDglNVk' as location_id",
        column_names=["datetime", "location_id"],
    )


def test_join_to_location_column_names(
    exemplar_spatial_unit_param, location_joinable_query
):
    """ Test that JoinToLocation's column_names property is accurate."""
    if not exemplar_spatial_unit_param.has_geography:
        pytest.skip("JoinToLocation does not accept CellSpatialUnit objects")
    joined = JoinToLocation(
        location_joinable_query, spatial_unit=exemplar_spatial_unit_param
    )
    assert joined.head(0).columns.tolist() == joined.column_names


def test_join_to_location_raises_value_error(test_query):
    """
    Test that JoinToLocation raises an InvalidSpatialUnitError if spatial_unit
    does not have geography information.
    """
    with pytest.raises(InvalidSpatialUnitError):
        JoinToLocation(test_query, spatial_unit=make_spatial_unit("cell"))


moving_sites = [
    "N0tfEoYN",
    "yPANTB8f",
    "DJHTx6HD",
    "aqt4bzQU",
    "PYTxb352",
    "hRU1j2q5",
    "UJE4phmX",
]

# Date at which the cells move
move_date = datetime(2016, 1, 6, tzinfo=timezone.utc)


def test_join_with_versioned_cells(get_dataframe, get_length):
    """
    Test that flowmachine_core.JoinToLocation can fetch the cell version.
    """
    ul = EventTableSubset(start="2016-01-05", stop="2016-01-07")
    df = get_dataframe(
        JoinToLocation(ul, spatial_unit=make_spatial_unit("versioned-cell"))
    )
    # As our database is complete we should not drop any rows
    assert len(df) == get_length(ul)
    # These should all be version zero, these are the towers before the changeover date, or those that
    # have not moved.
    should_be_version_zero = df[
        (df.datetime <= move_date) | (~df.location_id.isin(moving_sites))
    ]

    # These should all be one, they are the ones after the change over time that have moved.
    should_be_version_one = df[
        (df.datetime > move_date) & (df.location_id.isin(moving_sites))
    ]

    assert (should_be_version_zero.version == 0).all()
    assert (should_be_version_one.version == 1).all()


def test_join_with_lon_lat(get_dataframe):
    """
    Test that flowmachine_core.JoinToLocation can get the lon-lat values of the cell
    """
    ul = EventTableSubset(
        start="2016-01-05",
        stop="2016-01-07",
        columns=["msisdn", "datetime", "location_id"],
    )
    df = get_dataframe(JoinToLocation(ul, spatial_unit=make_spatial_unit("lon-lat")))

    expected_cols = sorted(["subscriber", "datetime", "location_id", "lon", "lat"])
    assert sorted(df.columns) == expected_cols
    # Pick out one cell that moves location and assert that the
    # lon-lats are right
    focal_cell = "dJb0Wd"
    lon1, lat1 = (83.09284486, 27.648837800000003)
    lon2, lat2 = (83.25769074752517, 27.661443318109132)
    post_move = df[(df.datetime > move_date) & (df["location_id"] == focal_cell)]
    pre_move = df[(df.datetime < move_date) & (df["location_id"] == focal_cell)]
    # And check them all one-by-one
    np.isclose(pre_move.lon, lon1).all()
    np.isclose(pre_move.lat, lat1).all()
    np.isclose(post_move.lon, lon2).all()
    np.isclose(post_move.lat, lat2).all()


def test_join_with_polygon(get_dataframe, get_length):
    """
    Test that flowmachine_core.JoinToLocation can get the (arbitrary) polygon
    of each cell.
    """
    ul = EventTableSubset(
        start="2016-01-05",
        stop="2016-01-07",
        columns=["msisdn", "datetime", "location_id"],
    )
    j = JoinToLocation(
        ul,
        spatial_unit=make_spatial_unit(
            "polygon",
            region_id_column_name="admin3pcod",
            geom_table="geography.admin3",
            geom_column="geom",
        ),
    )
    df = get_dataframe(j)

    expected_cols = sorted(["admin3pcod", "location_id", "subscriber", "datetime"])
    assert sorted(df.columns) == expected_cols
    assert len(df) == get_length(ul)


def test_join_to_admin(get_dataframe, get_length):
    """
    Test that flowmachine_core.JoinToLocation can join to a admin region.
    """
    ul = EventTableSubset(
        start="2016-01-05",
        stop="2016-01-07",
        columns=["msisdn", "datetime", "location_id"],
    )
    df = get_dataframe(
        JoinToLocation(ul, spatial_unit=make_spatial_unit("admin", level=3))
    )
    assert len(df) == get_length(ul)
    expected_cols = sorted(["subscriber", "datetime", "location_id", "pcod"])
    assert sorted(df.columns) == expected_cols


def test_join_to_grid(get_dataframe, get_length):
    """
    Test that we can join to a grid square
    """
    ul = EventTableSubset(start="2016-01-05", stop="2016-01-07")
    df = get_dataframe(
        JoinToLocation(ul, spatial_unit=make_spatial_unit("grid", size=50))
    )
    assert len(df) == get_length(ul)


def test_location_joined_query_return_type(exemplar_spatial_unit_param):
    """
    Test that location_joined_query(query, spatial_unit) returns a
    JoinToLocation object when spatial_unit != CellSpatialUnit(), and returns
    query when spatial_unit == CellSpatialUnit().
    """
    table = EventTableSubset(start="2016-01-05", stop="2016-01-07")
    joined = location_joined_query(table, spatial_unit=exemplar_spatial_unit_param)
    if make_spatial_unit("cell") == exemplar_spatial_unit_param:
        assert joined is table
    else:
        assert isinstance(joined, JoinToLocation)


def test_location_joined_query_raises_error():
    """
    Test that location_joined_query raises an error if spatial_unit is not a
    SpatialUnit object.
    """
    table = EventTableSubset(start="2016-01-05", stop="2016-01-07")
    with pytest.raises(InvalidSpatialUnitError):
        location_joined_query(table, spatial_unit="foo")
