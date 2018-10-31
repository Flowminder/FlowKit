# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the Query() base class.
"""

from unittest import TestCase
import os
import pandas as pd
from flowmachine.features.utilities.sets import EventTableSubset
from flowmachine.features.subscriber.daily_location import locate_subscribers
from flowmachine.core.query import Query


class test_to_sql(TestCase):
    """
    Test that queries can be stored back into the database, or as
    csvs.
    """

    def setUp(self):
        self.query = EventTableSubset("2016-01-01", "2016-01-01 01:00:00")
        self.c = Query.connection
        self.c.engine.execute("CREATE SCHEMA IF NOT EXISTS tests")

    def test_stores_table(self):
        """
        EventTableSubset().to_sql() can be stored as a TABLE.
        """
        self.query.to_sql(schema="tests", name="test_table")
        self.assertTrue(
            "test_table" in self.c.inspector.get_table_names(schema="tests")
        )
        self.c.engine.execute("DROP TABLE tests.test_table")

    def test_stores_view(self):
        """
        EventTableSubset().to_sql() can be stored as a VIEW.
        """
        self.query.to_sql(schema="tests", name="test_view", as_view=True)
        self.assertTrue("test_view" in self.c.inspector.get_view_names(schema="tests"))
        self.c.engine.execute("DROP VIEW tests.test_view")

    def test_can_force_rewrite(self):
        """
        Test that we can force the rewrite of a test to the database.
        """

        self.query.to_sql(schema="tests", name="test_rewrite")
        # We're going to delete everything from the table, then
        # force a rewrite, and check that the table now has data.
        sql = """DELETE FROM tests.test_rewrite"""
        self.c.engine.execute(sql)
        self.query.to_sql(schema="tests", name="test_rewrite", force=True)
        table_length = self.c.engine.execute(
            "SELECT count(*) FROM tests.test_rewrite"
        ).fetchall()[0][0]
        self.assertTrue(table_length > 1)
        self.c.engine.execute("DROP TABLE tests.test_rewrite")

    def tearDown(self):
        self.c.engine.execute("DROP SCHEMA tests CASCADE")
