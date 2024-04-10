# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for checking if the Oracle_fdw has been built correctly.
"""

import os

import pytest


@pytest.fixture(scope="module", autouse=True)
def port(env):
    """${ORACLE_DB_PORT}

    Returns
    -------
    str
        The ${ORACLE_DB_PORT}.
    """
    return env["ORACLE_DB_PORT"]


@pytest.mark.parametrize("extension", ["oracle_fdw"])
def test_oracle_fdw_available(pg_available_extensions, extension):
    """oracle_fdw is installed in oracle container."""
    assert extension in pg_available_extensions
