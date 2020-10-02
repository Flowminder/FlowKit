import pickle

import pytest

from flowmachine.core import Table
from flowmachine.core.errors.flowmachine_errors import QueryErroredException


@pytest.mark.parametrize("columns", [["msisdn", "id"]])
def test_table_column_names(columns):
    """Test that column_names property matches head(0) for tables"""
    t = Table("events.calls", columns=columns)
    assert t.head(0).columns.tolist() == t.column_names


@pytest.mark.parametrize(
    "args",
    [
        dict(name="events.calls", schema="extra_schema", columns=["id"]),
        dict(name="calls", schema="events", columns=None),
        dict(name="calls", schema="events", columns=[]),
    ],
)
def test_table_init(args):
    """
    Test that table creation handles params properly.
    """

    with pytest.raises(ValueError):
        Table(**args)


@pytest.mark.parametrize(
    "args",
    [
        dict(name="events.calls", columns=["NO SUCH COLUMN"]),
        dict(name="NO SUCH TABLE", columns=["id"]),
    ],
)
def test_table_preflight(args):
    with pytest.raises(QueryErroredException):
        Table(**args).preflight()


def public_schema_checked():
    """Test that where no schema is provided, public schema is checked."""
    t = Table("gambia_admin2", columns=["geom"]).preflight()


def test_children():
    """
    Test that table inheritance is correctly detected.
    """

    assert Table("events.calls", columns=["id"]).has_children()
    assert not Table("geography.admin3", columns=["geom"]).has_children()


def test_columns():
    """
    Test that table object gives the right columns.
    """
    t = Table("events.calls", columns="msisdn")
    assert t.get_dataframe().columns.tolist() == ["msisdn"]


def test_store_with_table():
    """
    Test that a subset of a table can be stored.
    """
    t = Table("events.calls", columns=["id"])
    s = t.subset("id", ["5wNJA-PdRJ4-jxEdG-yOXpZ", "5wNJA-PdRJ4-jxEdG-yOXpZ"])
    s.store().result()
    assert s.is_stored
    t.invalidate_db_cache()
    assert not s.is_stored
    assert t.is_stored


def test_get_table_is_self():
    """
    The get_table method on a Table should return itself.
    """
    t = Table("events.calls", columns=["id"])
    assert t.get_table() is t


def test_dependencies():
    """
    Check that a table without explicit columns has no other queries as a dependency,
    and a table with explicit columns has its parent table as a dependency.
    """
    t1 = Table("events.calls", columns=["id"])
    assert t1.dependencies == set()

    t2 = Table("events.calls", columns=["id"])
    assert len(t2.dependencies) == 1
    t2_parent = t2.dependencies.pop()
    assert "057addedac04dbeb1dcbbb6b524b43f0" == t2_parent.query_id


def test_subset():
    """
    Test that a subset of a table doesn't show as stored.
    """
    ss = Table("events.calls", columns=["id"]).subset(
        "id", ["5wNJA-PdRJ4-jxEdG-yOXpZ", "5wNJA-PdRJ4-jxEdG-yOXpZ"]
    )
    assert not ss.is_stored


def test_pickling():
    """
    Test that we can pickle and unpickle subset classes.
    """
    ss = Table("events.calls", columns=["id"]).subset(
        "id", ["5wNJA-PdRJ4-jxEdG-yOXpZ", "5wNJA-PdRJ4-jxEdG-yOXpZ"]
    )
    assert ss.get_query() == pickle.loads(pickle.dumps(ss)).get_query()
    assert ss.query_id == pickle.loads(pickle.dumps(ss)).query_id
