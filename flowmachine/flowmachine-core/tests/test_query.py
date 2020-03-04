# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the basic functionality of the base classes that do not
pertain to any one particular query
"""
from typing import List

import pytest
from sqlalchemy.exc import ProgrammingError

from query_bases.query import Query


def test_bad_sql_logged_and_raised(caplog):
    """SQL failures during a store should be logged, and raised."""

    class BadQuery(Query):
        def _make_query(self):
            return "THIS IS NOT VALID SQL"

        @property
        def column_names(self):
            return []

    with pytest.raises(ProgrammingError):
        fut = BadQuery().store()
        exec = fut.exception()
        raise exec
    assert "Error executing SQL" in caplog.messages[-1]


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

        @property
        def column_names(self) -> List[str]:
            return []

    o = inherits_object_representation()
    r = o.__repr__()
    assert r.startswith("<Query of type:")


def test_instantiating_base_class_raises_error():
    """
    Instantiating flowmachine_core.Query raises an error.
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

        @property
        def column_names(self) -> List[str]:
            return ["1"]

    sq = storable_query()
    sq.invalidate_db_cache()
    assert not sq.is_stored

    sq = storable_query()
    sq.store().result()
    assert sq.is_stored
    sq.invalidate_db_cache()


def test_return_table(test_query):
    """Test that we can return the table of a stored query."""
    test_query.store().result()
    assert (
        test_query.get_table().get_dataframe().values.tolist()
        == test_query.get_dataframe().values.tolist()
    )


def test_exception_on_unstored(test_query):
    """Test that an exception is raised when the query is not stored"""
    with pytest.raises(ValueError):
        test_query.get_table()


def test_iteration(test_query):
    """Test that we can iterate and it doesn't break hashing"""
    md5 = test_query.query_id
    for _ in test_query:
        pass
    assert md5 == test_query.query_id


def test_limited_head(test_query):
    """Test that we can call head on a query with a limit clause."""
    test_query.random_sample(size=2, sampling_method="bernoulli").head()


def test_make_sql_no_overwrite(test_query):
    """
    Test the Query._make_sql won't overwrite an existing table
    """

    assert [] == test_query._make_sql("admin3", schema="geography")


def test_query_formatting(test_query):
    """
    Test that query can be formatted as a string, with query attributes
    specified in the `fmt` argument being included.
    """
    assert "<Query of type: CustomQuery>" == format(test_query)
    assert (
        "<Query of type: CustomQuery, column_names: ['value']>"
        == f"{test_query:column_names}"
    )

    with pytest.raises(
        ValueError, match="Format string contains invalid query attribute: 'foo'"
    ):
        format(test_query, "query_id,foo")
