# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the Connection() class. 
"""
import datetime
from unittest.mock import Mock
import pytest


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

    assert flowmachine_connect.max_date().strftime("%Y-%m-%d") == "2016-01-07"
    assert (
        flowmachine_connect.max_date(table="all").strftime("%Y-%m-%d") == "2016-01-07"
    )


@pytest.mark.parametrize(
    "tables", [("calls",), ("calls", "sms"), ("calls", "sms", "mds", "topups")]
)
def test_multitable_availability(tables, flowmachine_connect):
    """Dict returned by available_dates should have keys for all requested tables."""
    for table in tables:
        assert isinstance(flowmachine_connect.available_dates[table], list)


def test_available_dates(flowmachine_connect):
    """Test that available dates returns correct ones."""
    assert "calls" in flowmachine_connect.available_dates
    assert "sms" in flowmachine_connect.available_dates
    assert datetime.date(2016, 1, 7) in flowmachine_connect.available_dates["calls"]
    assert datetime.date(2016, 9, 9) not in flowmachine_connect.available_dates["calls"]


def test_location_id(flowmachine_connect):
    """Test that we can get the location_id lookup table from the db."""
    assert "infrastructure.cells" == flowmachine_connect.location_table


def test_location_tables(flowmachine_connect):
    """Test that connection's location_tables attribute is correctly calculated"""
    assert sorted(["calls", "mds", "sms", "topups"]) == sorted(
        flowmachine_connect.location_tables
    )
