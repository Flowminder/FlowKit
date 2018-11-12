# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the Query() base class.
"""

import pytest

from flowmachine.core import Table
from flowmachine.features.utilities.sets import EventTableSubset


@pytest.fixture(autouse=True)
def test_table_schema(flowmachine_connect):
    """
    Fixture which creates a schema called 'tests' before every test
    and destroys it again after the test has finished.
    """
    flowmachine_connect.engine.execute("CREATE SCHEMA IF NOT EXISTS tests")
    yield
    flowmachine_connect.engine.execute("DROP SCHEMA tests CASCADE")


def test_stores_table(flowmachine_connect):
    """
    EventTableSubset().to_sql() can be stored as a TABLE.
    """
    query = EventTableSubset("2016-01-01", "2016-01-01 01:00:00")
    query.to_sql(schema="tests", name="test_table")
    assert "test_table" in flowmachine_connect.inspector.get_table_names(schema="tests")


def test_stores_view(flowmachine_connect):
    """
    EventTableSubset().to_sql() can be stored as a VIEW.
    """
    query = EventTableSubset("2016-01-01", "2016-01-01 01:00:00")
    query.to_sql(schema="tests", name="test_view", as_view=True)
    assert "test_view" in flowmachine_connect.inspector.get_view_names(schema="tests")


def test_can_force_rewrite(flowmachine_connect, get_length):
    """
    Test that we can force the rewrite of a test to the database.
    """
    query = EventTableSubset("2016-01-01", "2016-01-01 01:00:00")
    query.to_sql(schema="tests", name="test_rewrite")
    # We're going to delete everything from the table, then
    # force a rewrite, and check that the table now has data.
    sql = """DELETE FROM tests.test_rewrite"""
    flowmachine_connect.engine.execute(sql)
    assert 0 == get_length(Table("tests.test_rewrite"))
    query.to_sql(schema="tests", name="test_rewrite", force=True)
    assert 1 < get_length(Table("tests.test_rewrite"))
