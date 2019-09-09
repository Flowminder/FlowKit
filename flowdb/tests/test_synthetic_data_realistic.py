# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests the seeding of calls by the realistic seeder.
"""

def test_correct_calls_dates(cursor, env):
    """Check that synthetic data container contains only one day of call data."""
    query = "SELECT DISTINCT(datetime::date) FROM events.calls"
    cursor.execute(query)
    results = set([str(x["datetime"]) for x in cursor.fetchall()])
    expected = ["2016-01-01"]
    assert results == set(expected)

def test_correct_mds_dates(cursor, env):
    """Check that synthetic data container contains only one day of mds data."""
    query = "SELECT DISTINCT(datetime::date) FROM events.mds"
    cursor.execute(query)
    results = set([str(x["datetime"]) for x in cursor.fetchall()])
    expected = ["2016-01-01"]
    assert results == set(expected)

def test_correct_sms_dates(cursor, env):
    """Check that synthetic data container contains only one day of sms data."""
    query = "SELECT DISTINCT(datetime::date) FROM events.sms"
    cursor.execute(query)
    results = set([str(x["datetime"]) for x in cursor.fetchall()])
    expected = ["2016-01-01"]
    assert results == set(expected)

def test_correct_durations(cursor, env):
    """Check call durations are set correctly."""
    query = "SELECT DISTINCT(duration) FROM events.calls ORDER BY duration"
    cursor.execute(query)
    results = set([int(x["duration"]) for x in cursor.fetchall()])
    expected = [260,520,780,1040,1300,1560,1819,2080,2340,2600]
    print(results)
    assert results == set(expected)
