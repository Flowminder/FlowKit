# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the basic functionality of the base classes that do not
pertain to any one particular query
"""

import pytest
from psycopg2._psycopg import ProgrammingError

from flowmachine.core.query import Query
from flowmachine.features import daily_location


def test_bad_sql_logged_and_raised(caplog):
    """SQL failures during a store should be logged, and raised."""

    class BadQuery(Query):
        def _make_query(self):
            return "THIS IS NOT VALID SQL"

        @property
        def column_names(self):
            return []

    fut = BadQuery().store()
    assert isinstance(fut.exception(), ProgrammingError)
    assert "Error executing SQL" in caplog.messages[0]


def test_method_not_implemented():
    """
    Defining query without _make_query() method raises typeerror.
    """

    class inherits_for_raising_errors(Query):
        def make_query(self):
            pass

    with pytest.raises(TypeError):
        inherits_for_raising_errors()


def test_object_representation_is_correct():
    """
    Object representation __repr__ is correct.
    """

    class inherits_object_representation(Query):
        def _make_query(self):
            pass

    o = inherits_object_representation()
    r = o.__repr__()
    assert "Query object of type" in r


def test_instantiating_base_class_raises_error():
    """
    Instantiating flowmachine.Query raises an error.
    """

    with pytest.raises(TypeError):
        Query()


def test_is_stored():
    """
    Test the Query.is_stored returns the correct result.
    """

    class storable_query(Query):
        def _make_query(self):
            return """SELECT 1"""

    sq = storable_query()
    sq.invalidate_db_cache()
    assert not sq.is_stored

    sq = storable_query()
    sq.store().result()
    assert sq.is_stored
    sq.invalidate_db_cache()


def test_return_table():
    """Test that we can return the table of a stored query."""
    dl = daily_location("2016-01-01")
    dl.store().result()
    assert (
        dl.get_table().get_dataframe().values.tolist()
        == dl.get_dataframe().values.tolist()
    )


def test_exception_on_unstored():
    """Test that an exception is raised when the query is not stored"""
    dl = daily_location("2016-01-01")
    with pytest.raises(ValueError):
        dl.get_table()


def test_iteration():
    """Test that we can iterate and it doesn't break hashing"""
    dl = daily_location("2016-01-01")
    md5 = dl.md5
    for _ in dl:
        pass
    assert md5 == dl.md5


def test_limited_head():
    """Test that we can call head on a query with a limit clause."""
    dl = daily_location("2016-01-01")
    dl.random_sample(2).head()
