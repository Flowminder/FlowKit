# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Test the synthetic data.
"""

import pytest
import time


@pytest.fixture(scope="module", autouse=True)
def port(env):
    """${SYNTHETIC_DATA_DB_PORT}

    Returns
    -------
    str
        The ${SYNTHETIC_DATA_DB_PORT}.
    """
    return env["SYNTHETIC_DATA_DB_PORT"]


def test_correct_dates(cursor):
    """Check that synthetic data container contains the correct dates of data."""
    query = "SELECT DISTINCT(datetime::date) FROM events.calls"
    cursor.execute(query)
    results = set([str(x["datetime"]) for x in cursor.fetchall()])
    expected = ["2016-01-01", "2016-01-02", "2016-01-03"]
    assert results == set(expected)


def test_correct_num_calls(cursor):
    """Checking that synthetic data container contains the correct number of calls for each day."""
    query = """
    SELECT datetime::date, COUNT(*) as count
    FROM events.calls
    GROUP BY datetime::date
    """
    cursor.execute(query)
    results = set([i["count"] for i in cursor.fetchall()])
    assert results == set([4000])


def test_correct_cell_indexes(cursor):
    """Cells table should have five indexes - id, site_id, primary key and spatial ones on geom_point and geom_polygon."""
    expected_indexes = [
        {
            "index_definition": "CREATE INDEX cells_id_idx ON infrastructure.cells USING btree (id)"
        },
        {
            "index_definition": "CREATE INDEX cells_site_id_idx ON infrastructure.cells USING btree (site_id)"
        },
        {
            "index_definition": "CREATE INDEX infrastructure_cells_geom_point_index ON infrastructure.cells USING gist (geom_point)"
        },
        {
            "index_definition": "CREATE INDEX infrastructure_cells_geom_polygon_index ON infrastructure.cells USING gist (geom_polygon)"
        },
        {
            "index_definition": "CREATE UNIQUE INDEX cells_id_version_key ON infrastructure.cells USING btree (id, version)"
        },
        {
            "index_definition": "CREATE UNIQUE INDEX cells_pkey ON infrastructure.cells USING btree (cell_id)"
        },
    ]
    query = "select pg_get_indexdef(indexrelid) as index_definition from pg_index where indrelid = 'infrastructure.cells'::regclass ORDER BY index_definition;"
    cursor.execute(query)
    results = cursor.fetchall()
    assert expected_indexes == results


def test_correct_site_indexes(cursor):
    """Sites table should have four indexes - id, primary key
    and spatial ones on geom_point and geom_polygon.
    """
    expected_indexes = [
        {
            "index_definition": "CREATE INDEX infrastructure_sites_geom_point_index ON infrastructure.sites USING gist (geom_point)"
        },
        {
            "index_definition": "CREATE INDEX infrastructure_sites_geom_polygon_index ON infrastructure.sites USING gist (geom_polygon)"
        },
        {
            "index_definition": "CREATE INDEX sites_id_idx ON infrastructure.sites USING btree (id)"
        },
        {
            "index_definition": "CREATE UNIQUE INDEX sites_id_version_key ON infrastructure.sites USING btree (id, version)"
        },
        {
            "index_definition": "CREATE UNIQUE INDEX sites_pkey ON infrastructure.sites USING btree (site_id)"
        },
    ]
    query = "select pg_get_indexdef(indexrelid) index_definition from pg_index where indrelid = 'infrastructure.sites'::regclass ORDER BY index_definition;"
    cursor.execute(query)
    results = cursor.fetchall()
    assert expected_indexes == results


def test_correct_cells(cursor):
    """Checking that synthetic data container contains the correct number of cells."""
    query = """SELECT COUNT(*) FROM infrastructure.cells"""
    cursor.execute(query)
    results = cursor.fetchall()[0]["count"]
    assert results == 100


def test_cells_within_geoms(cursor):
    """Check synth cells are in correct location for nepal"""
    query = """SELECT st_x(geom_point) as lon, st_y(geom_point) as lat FROM infrastructure.cells"""
    cursor.execute(query)
    res = cursor.fetchall()[0]
    lon = res["lon"]
    lat = res["lat"]

    assert 80 <= lon <= 89
    assert 26 <= lat <= 31


def test_calls_registered_in_available_tables(cursor):
    """Make sure calls tables registered correctly"""
    query = """
    select
        *
    from
        available_tables
    where
        table_name = 'calls'
    """
    cursor.execute(query)
    res = cursor.fetchall()[0]

    assert res["has_locations"]
    assert res["has_subscribers"]
    assert res["has_counterparts"]
