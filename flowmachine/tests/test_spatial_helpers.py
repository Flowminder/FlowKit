# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest import TestCase
from flowmachine.core import JoinToLocation
from flowmachine.features.utilities import subscriber_locations
from flowmachine.features import CellToPolygon, CellToAdmin, CellToGrid
from pandas import DataFrame
from datetime import datetime, timezone
import numpy as np
import pytest


@pytest.mark.parametrize(
    "mapper, args",
    [
        (CellToAdmin, {"level": "admin3"}),
        (CellToAdmin, {"level": "admin3", "column_name": "admin3pcod"}),
        (
            CellToPolygon,
            {"column_name": "admin3name", "polygon_table": "geography.admin3"},
        ),
        (
            CellToPolygon,
            {
                "column_name": "id",
                "polygon_table": "infrastructure.sites",
                "geom_col": "geom_point",
            },
        ),
        (
            CellToPolygon,
            {
                "column_name": "id",
                "polygon_table": "SELECT * FROM infrastructure.sites",
                "geom_col": "geom_point",
            },
        ),
        (CellToGrid, {"size": 5}),
    ],
)
def test_cell_to_x_mapping_column_names(mapper, args):
    """Test that the CellToX mappers have accurate column_names properties."""
    instance = mapper(**args)
    assert instance.head(0).columns.tolist() == instance.column_names


def make_fake_table(con):
    """
    Makes a copy of the admin3 table, but with different
    names.
    """

    create_schema = "CREATE SCHEMA IF NOT EXISTS test_geog"

    create_table = """
            CREATE TABLE IF NOT EXISTS 
                test_geog.ad3 AS
                (SELECT geom AS geo,
                  admin3name AS naame
                FROM geography.admin3);
            """

    with con.begin() as trans:
        con.execute(create_schema)
        con.execute(create_table)


def drop_fake_table(con):

    with con.begin():
        con.execute("DROP TABLE test_geog.ad3")
        con.execute("DROP SCHEMA test_geog")


class TestCellPolygonMapping(TestCase):
    def setUp(self):

        self.mapping = CellToPolygon(
            column_name="naame", polygon_table="test_geog.ad3", geom_col="geo"
        )
        self.con = self.mapping.connection.engine
        make_fake_table(self.con)

    def tearDown(self):

        drop_fake_table(self.con)

    def test_returns_df(self):
        """
        CellAdminMapping().get_dataframe() returns a dataframe.
        """

        expected_cols = [
            "location_id",
            "version",
            "date_of_first_service",
            "date_of_last_service",
            "naame",
        ]
        df = self.mapping.get_dataframe()
        self.assertIs(type(df), DataFrame)
        self.assertEqual(list(df.columns), expected_cols)


class TestCellAdminMapping(TestCase):
    def test_cell_to_admin(self):
        """
        Test that CellToAdmin can return the admin level of all cells.
        """

        mapping = CellToAdmin(level="admin3")
        df = mapping.get_dataframe()
        self.assertIs(type(df), DataFrame)
        expected_cols = [
            "location_id",
            "version",
            "date_of_first_service",
            "date_of_last_service",
            "name",
        ]
        self.assertEqual(mapping.column_names, expected_cols)


class TestJoinToLocations(TestCase):
    """
    Tests for flowmachine.JoinToLocation
    """

    def setUp(self):

        # This gives us the subscribers locations over three day
        # in which some of those days towers change location
        self.ul = subscriber_locations("2016-01-05", "2016-01-07", level="cell")
        self.con = self.ul.connection.engine
        # These are the naught sites that move location
        self.moving_sites = [
            "N0tfEoYN",
            "yPANTB8f",
            "DJHTx6HD",
            "aqt4bzQU",
            "PYTxb352",
            "hRU1j2q5",
            "UJE4phmX",
        ]

        # Date at which the cells move
        self.move_date = datetime(2016, 1, 6, tzinfo=timezone.utc)

    def test_join_with_versioned_cells(self):
        """
        Test that flowmachine.JoinToLocation can fetch the cell version.
        """

        df = JoinToLocation(self.ul, level="versioned-cell").get_dataframe()
        # As our database is complete we should not drop any rows
        self.assertEqual(len(df), len(self.ul))
        # These should all be version zero, these are the towers before the changeover date, or those that
        # have not moved.
        should_be_version_zero = df[
            (df.time <= self.move_date) | (~df.location_id.isin(self.moving_sites))
        ]

        # These should all be one, they are the ones after the change over time that have moved.
        should_be_version_one = df[
            (df.time > self.move_date) & (df.location_id.isin(self.moving_sites))
        ]

        self.assertTrue((should_be_version_zero.version == 0).all())
        self.assertTrue((should_be_version_one.version == 1).all())

    def test_join_with_lat_lon(self):
        """
        Test that flowmachine.JoinToLocation can get the lat-lon values of the cell
        """

        df = JoinToLocation(self.ul, level="lat-lon").get_dataframe()
        self.assertIs(type(df), DataFrame)
        expected_cols = sorted(["subscriber", "time", "location_id", "lat", "lon"])
        self.assertEqual(sorted(df.columns), expected_cols)
        # Pick out one cell that moves location and assert that the
        # lat-lons are right
        focal_cell = "dJb0Wd"
        lat1, long1 = (27.648837800000003, 83.09284486)
        lat2, long2 = (27.661443318109132, 83.25769074752517)
        post_move = df[(df.time > self.move_date) & (df["location_id"] == focal_cell)]
        pre_move = df[(df.time < self.move_date) & (df["location_id"] == focal_cell)]
        # And check them all one-by-one
        np.isclose(pre_move.lat, lat1).all()
        np.isclose(pre_move.lon, long1).all()
        np.isclose(post_move.lat, lat2).all()
        np.isclose(post_move.lon, long2).all()

    def test_join_with_polygon(self):
        """
        Test that flowmachine.JoinToLocation can get the (arbitrary) polygon
        of each cell.
        """

        try:
            make_fake_table(self.con)
            j = JoinToLocation(
                self.ul,
                level="polygon",
                column_name="naame",
                polygon_table="test_geog.ad3",
                geom_col="geo",
            )
            df = j.get_dataframe()
        finally:
            drop_fake_table(self.con)

        self.assertIs(type(df), DataFrame)
        expected_cols = sorted(["subscriber", "time", "location_id", "naame"])
        self.assertEqual(sorted(df.columns), expected_cols)
        self.assertEqual(len(df), len(self.ul))

    def test_join_to_admin(self):
        """
        Test that flowmachine.JoinToLocation can join to a admin region.
        """

        df = JoinToLocation(self.ul, level="admin3").get_dataframe()
        self.assertEqual(len(df), len(self.ul))
        expected_cols = sorted(["subscriber", "time", "location_id", "name"])
        self.assertEqual(sorted(df.columns), expected_cols)

    def test_join_to_grid(self):
        """
        Test that we can join to a grid square
        """

        df = JoinToLocation(self.ul, level="grid", size=50).get_dataframe()
        self.assertEqual(len(df), len(self.ul))
