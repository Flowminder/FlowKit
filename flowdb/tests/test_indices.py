# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for indices in `flowdb`.
"""

import pytest


@pytest.fixture(params=["sites", "cells"])
def infrastructure_table(request):
    """Returns a infrastructure table name from the list of available names.

    Returns
    -------
    str
        The table name.
    """
    return request.param


@pytest.fixture(params=["point", "polygon"])
def expected_index(request, infrastructure_table):
    """Returns the name of expected index.

    Returns
    -------
    str
        The name of the expected index.
    """
    return f"infrastructure_{infrastructure_table}_geom_{request.param}_index"


@pytest.fixture()
def query(cursor, infrastructure_table):
    """Execute the query to find the index of the given infrastructure table,
    returning the index name.

    Returns
    -------
    str
        Returns the index name of the request infrastructure table.
    """
    sql = """
    SELECT
        *
    FROM pg_indexes
    WHERE schemaname = 'infrastructure' AND
          tablename = '{}';
    """.format(
        infrastructure_table
    )
    cursor.execute(sql)
    results = [i["indexname"] for i in cursor.fetchall()]
    return results


def test_infrastructure_index(query, expected_index):
    """infrastructure.* tables contain spatial indices."""
    assert expected_index in query
