# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests that fixuid works as expected
"""


def test_uid(docker_client, container_tag):
    """
    test that we can run the flowetl container with a specific user.
    Check UID is correct.
    """

    user = "1002:0"
    out = docker_client.containers.run(
        f"flowminder/flowetl:{container_tag}",
        "bash -c 'id -u'",
        user=user,
        environment={
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN": f"postgres://TEST_USER:TEST_PASSWORD@DUMMY_DB:5432/DUMMY_DB"
        },
        auto_remove=True,
    )
    assert out.decode("utf-8").strip() == "1002"


def test_uid_is_airflow(docker_client, container_tag):
    """
    Test that the user we run the container with is the airflow user.
    """

    user = "1002:0"
    out = docker_client.containers.run(
        f"flowminder/flowetl:{container_tag}",
        "bash -c 'id -u | id -nu'",
        user=user,
        environment={
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN": f"postgres://TEST_USER:TEST_PASSWORD@DUMMY_DB:5432/DUMMY_DB"
        },
        auto_remove=True,
    )
    assert out.decode("utf-8").strip() == "airflow"
