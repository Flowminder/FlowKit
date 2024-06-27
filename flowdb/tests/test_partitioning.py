# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for the database partitioning system.
"""

import pytest
import datetime as dt


@pytest.fixture
def test_tables():
    """Define the test tables for testing the flowmachine and flowapi user roles.

    Returns
    -------
    dict
        Dictionary whose key is the test table name and the value is the query for creating it.
    """
    dates = [dt.date(2017, 1, 1) + dt.timedelta(days=i) for i in range(10)]
    tables = {
        f"events.calls_{d.strftime('%Y%m%d')}": f"""
            CREATE TABLE IF NOT EXISTS events.calls_{d.strftime('%Y%m%d')} 
            PARTITION OF events.calls FOR VALUES FROM ('{d.strftime('%Y%m%d')}')
            TO ('{(d + dt.timedelta(days=1)).strftime('%Y%m%d')}');
            """
        for d in dates
    }

    return tables


@pytest.mark.usefixtures("create_test_tables")
def test_native_partitions(cursor, test_tables):
    """Native partitioning only scans filtered tables."""

    excluded_tables = set(test_tables.keys())
    excluded_tables.remove("events.calls_20170104")

    sql = """
        EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT count(*)
            FROM events.calls
                WHERE datetime >= '2017-01-04'::timestamptz AND
                      datetime < '2017-01-05'::timestamptz;
        """
    cursor.execute(sql)
    result = list(cursor.fetchall())[0]
    for plan in result["QUERY PLAN"][0]["Plan"]["Plans"][0]["Plans"]:
        assert not plan["Relation Name"] in [
            s.replace("events.", "") for s in excluded_tables
        ]


@pytest.mark.usefixtures("create_test_tables")
def test_dml_parallel(db_conn, cursor):
    """CREATE TABLE AS... should yield a parallel plan."""

    sql = """
        UPDATE pg_settings SET setting=0 WHERE name LIKE 'parallel%';
        UPDATE pg_settings SET setting=0 WHERE name = 'min_parallel_table_scan_size';
        EXPLAIN (FORMAT JSON)
            CREATE table foo AS (SELECT * FROM events.calls);
        """
    cursor.execute(sql)
    result = list(cursor.fetchall())[0]
    for plan in result["QUERY PLAN"][0]["Plan"]["Plans"][0]["Plans"]:
        assert plan["Parallel Aware"] == True
