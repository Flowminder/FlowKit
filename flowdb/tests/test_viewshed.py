# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Test that utility functions are installed and working properly.
"""
import pytest


@pytest.fixture
def test_tables():
    """Defines a test table for testing the utility functions.

    Returns
    -------
    dict
        Dictionary whose key is the test table name and the value is the query for creating it.
    """
    tables = {
        "viewshed_count_test_table": """
            CREATE TABLE viewshed_count_test_table(n numeric, od_line_identifier text);
            INSERT INTO viewshed_count_test_table
                (n, od_line_identifier)
                VALUES
                    (2, '2'),
                    (4, 'foo'),
                    (5, 'bar'),
                    (60, '100');
            """,
        "viewshed_visible_test": """
            CREATE TABLE viewshed_visible_test(location_id text, geom_point geometry, 
                od_line_identifier text, geom geometry, distance numeric, slope numeric);
            INSERT INTO viewshed_visible_test
                (location_id, od_line_identifier, distance, slope)
                VALUES
                    ('1', 'd4925957-2383-43c3-b332-5dc1dcde6cae', 10, 10),
                    ('1', 'd4925957-2383-43c3-b332-5dc1dcde6cae', 10, 9);
        """,
    }
    return tables


@pytest.mark.usefixtures("create_test_tables")
def test_viewshed_count(cursor):
    """Test that _viewshed_count_total returns a correct result."""
    sql = "SELECT _viewshed_count_total('viewshed_count_test_table')"
    cursor.execute(sql)
    vals = cursor.fetchall()[0]
    assert 4 == vals["_viewshed_count_total"]


@pytest.mark.usefixtures("create_test_tables")
def test_viewshed_visible(cursor):
    """Test that _visible correctly calculates some visible points."""
    sql = "SELECT * FROM _visible('viewshed_visible_test', 'd4925957-2383-43c3-b332-5dc1dcde6cae')"
    cursor.execute(sql)
    vals = cursor.fetchall()
    assert vals[0]["visible"]
    assert not vals[1]["visible"]


@pytest.mark.usefixtures("create_test_tables")
def test_viewshed(cursor):
    """Test that viewshed correctly calculates visible points"""
    sql = "SELECT * FROM viewshed('viewshed_visible_test')"
    cursor.execute(sql)
    vals = cursor.fetchall()
    assert vals[0]["visible"]
    assert not vals[1]["visible"]
    assert 2 == len(vals)
