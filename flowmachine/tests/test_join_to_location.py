# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from datetime import datetime, timezone

import numpy as np

from flowmachine.features import subscriber_locations
from flowmachine.core import JoinToLocation, location_joined_query
from flowmachine.core.spatial_unit import make_spatial_unit


def test_join_to_location_column_names(exemplar_spatial_unit_param):
    """ Test that JoinToLocation's column_names property is accurate."""
    if make_spatial_unit("cell") == exemplar_spatial_unit_param:
        pytest.skip("JoinToLocation does not accept CellSpatialUnit objects")
    table = subscriber_locations(
        "2016-01-05", "2016-01-07", spatial_unit=make_spatial_unit("cell")
    )
    joined = JoinToLocation(table, spatial_unit=exemplar_spatial_unit_param)
    assert joined.head(0).columns.tolist() == joined.column_names


def test_join_to_location_raises_value_error():
    """
    Test that JoinToLocation raises a ValueError if spatial_unit==CellSpatialUnit().
    """
    with pytest.raises(ValueError):
        table = subscriber_locations(
            "2016-01-05", "2016-01-07", spatial_unit=make_spatial_unit("cell")
        )
        joined = JoinToLocation(table, spatial_unit=make_spatial_unit("cell"))


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
    Test that flowmachine.JoinToLocation can fetch the cell version.
    """
    ul = subscriber_locations(
        "2016-01-05", "2016-01-07", spatial_unit=make_spatial_unit("cell")
    )
    df = get_dataframe(
        JoinToLocation(ul, spatial_unit=make_spatial_unit("versioned-cell"))
    )
    # As our database is complete we should not drop any rows
    assert len(df) == get_length(ul)
    # These should all be version zero, these are the towers before the changeover date, or those that
    # have not moved.
    should_be_version_zero = df[
        (df.time <= move_date) | (~df.location_id.isin(moving_sites))
    ]

    # These should all be one, they are the ones after the change over time that have moved.
    should_be_version_one = df[
        (df.time > move_date) & (df.location_id.isin(moving_sites))
    ]

    assert (should_be_version_zero.version == 0).all()
    assert (should_be_version_one.version == 1).all()


def test_join_with_lat_lon(get_dataframe):
    """
    Test that flowmachine.JoinToLocation can get the lat-lon values of the cell
    """
    ul = subscriber_locations(
        "2016-01-05", "2016-01-07", spatial_unit=make_spatial_unit("cell")
    )
    df = get_dataframe(JoinToLocation(ul, spatial_unit=make_spatial_unit("lat-lon")))

    expected_cols = sorted(["subscriber", "time", "location_id", "lat", "lon"])
    assert sorted(df.columns) == expected_cols
    # Pick out one cell that moves location and assert that the
    # lat-lons are right
    focal_cell = "dJb0Wd"
    lat1, long1 = (27.648837800000003, 83.09284486)
    lat2, long2 = (27.661443318109132, 83.25769074752517)
    post_move = df[(df.time > move_date) & (df["location_id"] == focal_cell)]
    pre_move = df[(df.time < move_date) & (df["location_id"] == focal_cell)]
    # And check them all one-by-one
    np.isclose(pre_move.lat, lat1).all()
    np.isclose(pre_move.lon, long1).all()
    np.isclose(post_move.lat, lat2).all()
    np.isclose(post_move.lon, long2).all()


def test_join_with_polygon(get_dataframe, get_length):
    """
    Test that flowmachine.JoinToLocation can get the (arbitrary) polygon
    of each cell.
    """
    ul = subscriber_locations(
        "2016-01-05", "2016-01-07", spatial_unit=make_spatial_unit("cell")
    )
    j = JoinToLocation(
        ul,
        spatial_unit=make_spatial_unit(
            "polygon",
            region_id_column_name="admin3pcod",
            polygon_table="geography.admin3",
            geom_column="geom",
        ),
    )
    df = get_dataframe(j)

    expected_cols = sorted(["admin3pcod", "location_id", "subscriber", "time"])
    assert sorted(df.columns) == expected_cols
    assert len(df) == get_length(ul)


def test_join_to_admin(get_dataframe, get_length):
    """
    Test that flowmachine.JoinToLocation can join to a admin region.
    """
    ul = subscriber_locations(
        "2016-01-05", "2016-01-07", spatial_unit=make_spatial_unit("cell")
    )
    df = get_dataframe(
        JoinToLocation(ul, spatial_unit=make_spatial_unit("admin", level=3))
    )
    assert len(df) == get_length(ul)
    expected_cols = sorted(["subscriber", "time", "location_id", "pcod"])
    assert sorted(df.columns) == expected_cols


def test_join_to_grid(get_dataframe, get_length):
    """
    Test that we can join to a grid square
    """
    ul = subscriber_locations(
        "2016-01-05", "2016-01-07", spatial_unit=make_spatial_unit("cell")
    )
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
    table = subscriber_locations(
        "2016-01-05", "2016-01-07", spatial_unit=make_spatial_unit("cell")
    )
    joined = location_joined_query(table, spatial_unit=exemplar_spatial_unit_param)
    if make_spatial_unit("cell") == exemplar_spatial_unit_param:
        assert joined is table
    else:
        assert isinstance(joined, JoinToLocation)
