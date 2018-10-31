# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests the flowdb_version() function.
"""


def test_connection(cursor, env):
    """flowdb_version() returns the right version number and release date."""
    cursor.execute("SELECT * FROM flowdb_version();")
    results = {"version": None, "release_date": None}
    results = list(cursor.fetchall())[0]
    assert results["version"] == env["FLOWDB_VERSION"]
    assert results["release_date"].strftime("%Y-%m-%d") == env["FLOWDB_RELEASE_DATE"]
