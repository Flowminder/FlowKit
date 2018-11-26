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
