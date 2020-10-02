# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the Query() base class.
"""

import pytest

from flowmachine.core import Table
from flowmachine.core.context import get_db
from flowmachine.features.utilities.event_table_subset import EventTableSubset


@pytest.fixture(autouse=True)
def test_table_schema(flowmachine_connect):
    """
    Fixture which creates a schema called 'tests' before every test
    and destroys it again after the test has finished.
    """
    get_db().engine.execute("CREATE SCHEMA IF NOT EXISTS tests")
    yield
    get_db().engine.execute("DROP SCHEMA tests CASCADE")


def test_stores_table(flowmachine_connect):
    """
    EventTableSubset.to_sql() can be stored as a TABLE.
    """
    query = EventTableSubset(start="2016-01-01", stop="2016-01-01 01:00:00")
    query.to_sql(name="test_table", schema="tests").result()
    assert get_db().has_table(name="test_table", schema="tests")


def test_can_force_rewrite(flowmachine_connect, get_length):
    """
    Test that we can force the rewrite of a test to the database.
    """
    query = EventTableSubset(start="2016-01-01", stop="2016-01-01 01:00:00")
    query.to_sql(name="test_rewrite", schema="tests").result()
    # We're going to delete everything from the table, then
    # force a rewrite, and check that the table now has data.
    sql = """DELETE FROM tests.test_rewrite"""
    get_db().engine.execute(sql)
    assert 0 == get_length(Table("tests.test_rewrite", columns=query.column_names))
    query.invalidate_db_cache(name="test_rewrite", schema="tests")
    query.to_sql(name="test_rewrite", schema="tests").result()
    assert 1 < get_length(Table("tests.test_rewrite", columns=query.column_names))
