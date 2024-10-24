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

from flowmachine.core import make_spatial_unit
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
            return """SELECT 1 as col"""

        @property
        def column_names(self) -> List[str]:
            return ["col"]

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
    md5 = dl.query_id
    for _ in dl:
        pass
    assert md5 == dl.query_id


def test_limited_head():
    """Test that we can call head on a query with a limit clause."""
    dl = daily_location("2016-01-01")
    dl.random_sample(size=2, sampling_method="bernoulli").head()


def test_make_sql_no_overwrite():
    """
    Test the Query._make_sql won't overwrite an existing table
    """

    dl = daily_location("2016-01-01")
    assert [] == dl._make_sql("admin3", schema="geography")


def test_query_formatting():
    """
    Test that query can be formatted as a string, with query attributes
    specified in the `fmt` argument being included.
    """
    dl = daily_location(
        "2016-01-01", spatial_unit=make_spatial_unit("cell"), method="last"
    )
    assert "<Query of type: LastLocation>" == format(dl)
    assert (
        "<Query of type: LastLocation, spatial_unit: CellSpatialUnit(), column_names: ['subscriber', 'location_id']>"
        == f"{dl:spatial_unit,column_names}"
    )

    with pytest.raises(
        ValueError, match="Format string contains invalid query attribute: 'foo'"
    ):
        format(dl, "query_id,foo")
