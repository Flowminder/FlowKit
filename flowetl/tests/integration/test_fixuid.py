# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests that fixuid works as expected
"""


def test_uid(docker_client, flowetl_tag):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = docker_client.containers.run(
        f"flowminder/flowetl:{flowetl_tag}", "bash -c 'id -u'", user=user
    )
    assert out.decode("utf-8").strip() == "1002"


def test_gid(docker_client, flowetl_tag):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = docker_client.containers.run(
        f"flowminder/flowetl:{flowetl_tag}", "bash -c 'id -g'", user=user
    )
    assert out.decode("utf-8").strip() == "1003"


def test_uid_is_airflow(docker_client, flowetl_tag):
    """
    test that we can run the flowetl container with a specific user
    """

    user = "1002:1003"
    out = docker_client.containers.run(
        f"flowminder/flowetl:{flowetl_tag}", "bash -c 'id -u | id -nu'", user=user
    )
    assert out.decode("utf-8").strip() == "airflow"
