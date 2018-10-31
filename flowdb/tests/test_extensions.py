# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for checking if database extensions have
been correctly installed.
"""

import pytest


@pytest.fixture()
def activate_pg_cron(cursor):
    """Activate the `pg_cron` extension if it has not been activated yet."""
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")


@pytest.mark.parametrize(
    "extension", ["postgis", "file_fdw", "uuid-ossp", "pgrouting", "pldbgapi"]
)
def test_extension_available(pg_available_extensions, extension):
    """Extension is installed."""
    assert extension in pg_available_extensions


def test_oracle_fdw_unavailable(pg_available_extensions):
    """oracle_fdw is not installed."""
    assert "oracle_fdw" not in pg_available_extensions


@pytest.mark.parametrize("shared_preload_library", ["pg_cron", "pg_stat_statements"])
def test_preload_libraries_available(shared_preload_libraries, shared_preload_library):
    """Shared pre-load library is installed."""
    assert shared_preload_library in shared_preload_libraries


def test_pg_cron_is_installed(activate_pg_cron, cursor):
    """pg_cron is installed and activated."""
    cursor.execute("SELECT cron.schedule('0 10 * * *', 'VACUUM');")
    results = []
    for i in cursor.fetchall():
        results.append(i)

    cursor.execute("SELECT cron.unschedule({})".format(results[0]["schedule"]))
    results = []
    for i in cursor.fetchall():
        results.append(i)

    assert results[0]["unschedule"]
