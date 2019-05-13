# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests that fixuid works as expected
"""


def test_uid(run_command_in_flowetl_container):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = run_command_in_flowetl_container("bash -c 'id -u'", user=user)
    assert out == "1002"


def test_gid(run_command_in_flowetl_container):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = run_command_in_flowetl_container("bash -c 'id -g'", user=user)
    assert out == "1003"


def test_uid_is_airflow(run_command_in_flowetl_container):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = run_command_in_flowetl_container("bash -c 'id -u | id -nu'", user=user)
    assert out == "airflow"
