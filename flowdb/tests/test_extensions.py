# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for checking if database extensions have
been correctly installed.
"""

import pytest


@pytest.mark.parametrize(
    "extension",
    [
        "postgis",
        "postgis_raster",
        "file_fdw",
        "uuid-ossp",
        "pgrouting",
        "pldbgapi",
        "pg_median_utils",
        "ogr_fdw",
        "tds_fdw"
    ],
)
def test_extension_available(pg_available_extensions, extension):
    """Extension is installed."""
    assert extension in pg_available_extensions


def test_oracle_fdw_unavailable(pg_available_extensions):
    """oracle_fdw is not installed."""
    assert "oracle_fdw" not in pg_available_extensions


@pytest.mark.parametrize("shared_preload_library", ["pg_stat_statements"])
def test_preload_libraries_available(shared_preload_libraries, shared_preload_library):
    """Shared pre-load library is installed."""
    assert shared_preload_library in shared_preload_libraries
