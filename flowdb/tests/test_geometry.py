# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for the geometry properties in PostGIS.
"""

import pytest


@pytest.fixture(params=["infrastructure.cells", "infrastructure.sites"])
def fqn_table(request):
    """Fully qualified name of the infrastructure table from the list.

    Returns
    -------
    str
        The fully qualified name of the infrastructure table.
    """
    return request.param


@pytest.fixture(params=["geom_point", "geom_polygon"])
def column(request):
    """Column from one of the infrastructure tables.

    Returns
    -------
    str
        The infrastructure table column name.
    """
    return request.param


def test_srid(cursor, fqn_table, column):
    """SRID is 4326 in all geometry columns."""
    schema, table = fqn_table.split(".")
    sql = """
    SELECT * FROM Find_SRID('{schema}', '{table}', '{column}') AS srid
    """.format(
        schema=schema, table=table, column=column
    )
    cursor.execute(sql)
    results = []
    for i in cursor.fetchall():
        results.append(i["srid"])
    assert results[0] == 4326
