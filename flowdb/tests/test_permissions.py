# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for the database permissions on the host when mounted as a Docker
container. The tests herein check that the `flowdb` Docker container contains
the same permissions as stipulated by environment variables.
"""
import os
from grp import getgrgid
from pwd import getpwuid

import pytest


@pytest.fixture(scope="module")
def owner_flowdb_data_dir(env):
    """${FLOWDB_DATA_DIR} ownership information.

    Returns
    -------
    dict
        A dictionary with ownership informantion, namely path, uid, uname, gid and gname.
    """
    # See http://stackoverflow.com/questions/1830618/\
    # how-to-find-the-owner-of-a-file-or-directory-in-python
    data_dir = env["FLOWDB_DATA_DIR"]
    owner = {
        "path": data_dir,
        "uid": os.stat(data_dir).st_uid,
        "uname": getpwuid(os.stat(data_dir).st_uid).pw_name,
        "gid": os.stat(data_dir).st_gid,
        "gname": getgrgid(os.stat(data_dir).st_gid).gr_name,
    }
    return owner


@pytest.mark.skipif(
    os.getenv("POSTGRES_UID") is None,
    reason="Environment variable $POSTGRES_UID not present.",
)
def test_host_group_permissions_same_as_variable(owner_flowdb_data_dir, env):
    """Host permissions for USER same as variable POSTGRES_UID."""
    assert owner_flowdb_data_dir["uid"] == int(env["POSTGRES_UID"])


@pytest.mark.skipif(
    os.getenv("POSTGRES_GID") is None,
    reason="Environment variable $POSTGRES_GID not present.",
)
def test_host_user_permissions_same_as_variable(owner_flowdb_data_dir, env):
    """Host permissions for GROUP same as variable POSTGRES_GID."""
    assert owner_flowdb_data_dir["gid"] == int(env["POSTGRES_GID"])
