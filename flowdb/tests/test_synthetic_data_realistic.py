# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests the seeding of calls by the realistic seeder.
"""


def test_connection(cursor, env):
    """Check that synthetic data container contains only one day of data."""
    query = "SELECT DISTINCT(datetime::date) FROM events.calls"
    cursor.execute(query)
    results = set([str(x["datetime"]) for x in cursor.fetchall()])
    expected = ["2016-01-01"]
    assert results == set(expected)
