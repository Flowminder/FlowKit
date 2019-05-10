# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests that fixuid works as expected
"""


def test_uid(flowetl_container):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = flowetl_container.exec_run("bash -c 'id -u'", user=user).output.decode(
        "utf-8"
    )
    assert out.strip() == "1002"


def test_gid(flowetl_container):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = flowetl_container.exec_run("bash -c 'id -g'", user=user).output.decode(
        "utf-8"
    )
    assert out.strip() == "1003"
