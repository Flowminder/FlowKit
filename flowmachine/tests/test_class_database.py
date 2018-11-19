# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the Connection() class. 
"""
import datetime
from unittest.mock import Mock

import psycopg2 as pg

from unittest import TestCase

import pytest

from flowmachine.core import Connection, Query


@pytest.fixture
def test_tables(flowmachine_connect):
    """
    Private method that creates a test table
    and adds data into it.
    """
    q = """
    BEGIN;
        CREATE TABLE IF NOT EXISTS test_table_a (
            id NUMERIC PRIMARY KEY,
            field TEXT
        );
        INSERT INTO test_table_a VALUES ('1', 'foo') ON CONFLICT (id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS test_table_b (
            id NUMERIC PRIMARY KEY,
            field TEXT,
            numeric_field NUMERIC
        );
        INSERT INTO test_table_b VALUES ('1', 'foo', '300') ON CONFLICT (id) DO NOTHING;
    END;
    """
    flowmachine_connect.engine.execute(q)
    yield flowmachine_connect
    q = """
            DROP TABLE IF EXISTS test_table_a;
            DROP TABLE IF EXISTS test_table_b;
        """
    flowmachine_connect.engine.execute(q)


def test_fetches_query_from_database(test_tables):
    """
    Connection().fetch() returns list of results from query.
    """
    q = "SELECT * FROM test_table_a;"
    r = test_tables.fetch(query=q)

    assert isinstance(r, list)


def test_tables_returns_list_of_tables(test_tables):
    """
    Connection().tables() returns correct table list.
    """
    #
    #  List of tables returned is
    #  accurate.
    #
    r = test_tables.tables()
    assert isinstance(r, list)

    #
    #  Regex function
    #  is working as expected.
    #
    r = test_tables.tables(regex="_a$")

    assert "test_table_a" in r
    assert "test_table_b" not in r


def test_columns_returns_column_list(test_tables):
    """
    Connection().columns() returns column list.
    """
    r = test_tables.columns(tables="test_table_a")

    assert isinstance(r, dict)
    assert "field" in r["test_table_a"]

    r = test_tables.columns(tables=["test_table_a", "test_table_b"])
    assert isinstance(r, dict)
    assert "field" in r["test_table_a"]
    assert "numeric_field" in r["test_table_b"]


def test_has_date(flowmachine_connect):
    """
    Test that the connection knows which dates it has.
    """

    assert flowmachine_connect.has_date(datetime.date(2016, 1, 7), "calls")
    assert not flowmachine_connect.has_date(datetime.date(2016, 1, 8), "calls")


def test_min_date(flowmachine_connect):
    """
    Test connection.min_date
    """

    assert flowmachine_connect.min_date().strftime("%Y-%m-%d") == "2016-01-01"
    assert (
        flowmachine_connect.min_date(table="all").strftime("%Y-%m-%d") == "2016-01-01"
    )


def test_max_date(flowmachine_connect):
    """
    Test connection.max_date
    """

    assert flowmachine_connect.max_date().strftime("%Y-%m-%d") == "2016-09-09"
    assert (
        flowmachine_connect.max_date(table="all").strftime("%Y-%m-%d") == "2016-09-09"
    )


def test_strict_max_date(flowmachine_connect):
    """
    Test connection.max_date
    """

    assert (
        flowmachine_connect.max_date(strictness=2).strftime("%Y-%m-%d") == "2016-01-07"
    )
    assert (
        flowmachine_connect.max_date(strictness=1).strftime("%Y-%m-%d") == "2016-01-07"
    )


@pytest.mark.parametrize(
    "tables", [("calls",), ("calls", "sms"), ("calls", "sms", "mds", "topups")]
)
def test_multitable_availability(tables, flowmachine_connect):
    """Dict returned by available_dates should have keys for all requested tables."""
    assert tables == tuple(flowmachine_connect.available_dates(table=tables).keys())


@pytest.mark.parametrize(
    "tables", [(1,), ("calls", 1), ["calls", "sms", "mds", "topups"], 0.1, 1]
)
def test_bad_table_inputs_to_available_dates(tables, flowmachine_connect):
    """TypeError should be raised for bad inputs to available_dates."""
    with pytest.raises(TypeError):
        flowmachine_connect.available_dates(table=tables)


def test_available_dates(flowmachine_connect):
    """Test that available dates returns correct ones."""
    assert "calls" in flowmachine_connect.available_dates()
    assert "sms" in flowmachine_connect.available_dates(table="sms")
    assert (
        datetime.datetime(2016, 1, 7, 0, 0)
        in flowmachine_connect.available_dates()["calls"]
    )
    assert (
        datetime.datetime(2016, 1, 7, 0, 0)
        not in flowmachine_connect.available_dates(stop="2016-01-05")["calls"]
    )
    assert (
        datetime.datetime(2016, 1, 1, 0, 0)
        not in flowmachine_connect.available_dates(start="2016-01-05")["calls"]
    )
    assert (
        datetime.datetime(2016, 1, 5, 0, 0)
        in flowmachine_connect.available_dates(start="2016-01-05")["calls"]
    )
    assert (
        datetime.datetime(2016, 1, 7, 0, 0)
        in flowmachine_connect.available_dates(start="2016-01-05", stop="2016-01-07")[
            "calls"
        ]
    )


def test_strict_available_dates(flowmachine_connect):
    """Test strict date availability."""
    assert (
        datetime.datetime(2016, 9, 9, 0, 0)
        in flowmachine_connect.available_dates()["calls"]
    )
    assert (
        datetime.datetime(2016, 9, 9, 0, 0)
        not in flowmachine_connect.available_dates(strictness=1)["calls"]
    )
    assert (
        datetime.datetime(2016, 9, 9, 0, 0)
        not in flowmachine_connect.available_dates(strictness=2)["calls"]
    )


def test_error_on_bad_strictness(flowmachine_connect):
    """Test that available dates errors for bad strictness levels."""
    with pytest.raises(ValueError):
        flowmachine_connect.available_dates(strictness="horatio")


def test_location_id(flowmachine_connect):
    """Test that we can get the location_id lookup table from the db."""
    assert "infrastructure.cells" == flowmachine_connect.location_table


def test_location_tables(flowmachine_connect):
    """Test that connection's location_tables attribute is correctly calculated"""
    assert sorted(["calls", "mds", "sms", "topups"]) == sorted(
        flowmachine_connect.location_tables
    )


def test_has_date_bad_estimate(flowmachine_connect, monkeypatch):
    """Test that event with a bad row count estimate, we can throw out a date."""

    monkeypatch.setattr(
        "flowmachine.core.Table.estimated_rowcount", Mock(return_value=1)
    )
    assert not flowmachine_connect.has_date(datetime.date(2016, 9, 9), "calls")
