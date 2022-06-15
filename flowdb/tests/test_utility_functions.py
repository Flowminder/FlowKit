# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Test that utility functions are installed and working properly.
"""
import pytest
import numpy as np


@pytest.fixture
def test_tables():
    """Defines a test table for testing the utility functions.

    Returns
    -------
    dict
        Dictionary whose key is the test table name and the value is the query for creating it.
    """
    tables = {
        "test_table": """
            CREATE TABLE
                test_table(
                    n NUMERIC,
                    text_number TEXT
                    );
            INSERT INTO test_table
                (n, text_number)
                VALUES
                    (2, '2'),
                    (4, 'foo'),
                    (5, 'bar'),
                    (60, '100');
            """
    }
    return tables


@pytest.mark.usefixtures("create_test_tables")
def test_isnumeric(cursor):
    """isnumeric() function returns correct casting."""
    sql = "SELECT isnumeric(text_number) AS result FROM test_table"
    cursor.execute(sql)
    results = [i["result"] for i in cursor.fetchall()]
    assert np.count_nonzero(results) == 2


def test_long_running_queries(cursor):
    """long_running_queries() returns list of long running queries."""
    sql = "SELECT * FROM long_running_queries()"
    cursor.execute(sql)
    results = [i["result"] for i in cursor.fetchall()]
    assert len(results) == 0


def test_close_all_connections(cursor):
    """close_all_connections() closes all connections, except this one."""
    sql = "SELECT close_all_connections() as result"
    cursor.execute(sql)
    results = [i["result"] for i in cursor.fetchall()]
    assert isinstance(results[0], int)


def test_available_tables(cursor):
    """available_tables() returns all events.* tables with two false columns."""
    sql = "SELECT * from available_tables()"
    cursor.execute(sql)
    table = {}
    for row in cursor.fetchall():
        for k, v in row.items():
            try:
                table[k].append(v)
            except KeyError:
                table[k] = [v]
    for t in ["calls", "forwards", "sms", "mds", "topups"]:
        assert t in table["table_name"]
    assert not any(table["has_locations"])
    assert not any(table["has_subscribers"])


def test_seeded_random_ints(cursor):
    """Seeded random integers should return some predictable outputs."""
    sql = "SELECT * from random_ints(0, 5, 10)"
    cursor.execute(sql)
    first_vals = [x["id"] for x in cursor.fetchall()]
    cursor.execute(sql)
    second_vals = [x["id"] for x in cursor.fetchall()]
    assert first_vals == second_vals


def test_random_ints_n_samples(cursor):
    """random_ints should return the requested number of random integers."""
    sql = "SELECT * from random_ints(0, 5, 10)"
    cursor.execute(sql)
    vals = [x["id"] for x in cursor.fetchall()]
    assert len(vals) == 5


def test_seeded_random_ints_seed_reset(cursor):
    """Seeded random integers should not leave the seed the same after running."""
    sql = """
        SELECT * from random_ints(0, 5, 10);
        SELECT * from random()
    """
    cursor.execute(sql)
    first_sample = cursor.fetchall()[0]["random"]
    cursor.execute(sql)
    second_sample = cursor.fetchall()[0]["random"]
    assert first_sample != second_sample


def test_grid(cursor):
    """Test that grid function returns the grid we expect."""
    sql = """
            SELECT
            count(*),
                avg(ST_AREA(geom)),
                stddev(ST_AREA(geom))
                
            FROM
                (SELECT (
                    ST_Dump(makegrid_2d(ST_MakeEnvelope(0, 0, 10, 10, 3857),
                     1) -- cell step in meters
                 )).geom AS geom)
                 AS q_grid
            """
    cursor.execute(sql)
    vals = cursor.fetchall()[0]
    assert 121 == vals["count"]
    assert 1 == vals["avg"]
    assert 0 == vals["stddev"]
