# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests that fixuid works as expected
"""


def test_uid(
    flowetl_db_container, docker_client, container_tag, container_network, container_env
):
    """
    test that we can run the flowetl container with a specific user.
    Check UID is correct.
    """
    flowetl_db_container()
    user = "1002:0"
    out = docker_client.containers.run(
        f"flowminder/flowetl:{container_tag}",
        "bash -c 'id -u'",
        user=user,
        environment=container_env["flowetl"],
        auto_remove=True,
        network=container_network.name,
    )
    print(out)
    assert out.decode("utf-8").strip() == "1002"


def test_uid_is_airflow(
    flowetl_db_container, docker_client, container_tag, container_network, container_env
):
    """
    Test that the user we run the container with is the airflow user.
    """
    flowetl_db_container()
    user = "1002:0"
    out = docker_client.containers.run(
        f"flowminder/flowetl:{container_tag}",
        "bash -c 'id -u | id -nu'",
        user=user,
        auto_remove=True,
        environment=container_env["flowetl"],
        network=container_network.name,
    )
    assert out.decode("utf-8").strip() == "airflow"
