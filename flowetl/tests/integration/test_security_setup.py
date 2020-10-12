# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import docker

import pytest


@pytest.mark.parametrize(
    "env_var_to_remove",
    ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"],
)
def test_required_env_var(env_var_to_remove, docker_client, container_tag):
    """
    Test that an error is raised when one of the required env vars is not set.
    """
    env = {
        "AIRFLOW__CORE__SQL_ALCHEMY_CONN": f"postgres://TEST_USER:TEST_PASSWORD@DUMMY_DB:5432/DUMMY_DB",
    }
    env.pop(env_var_to_remove)
    with pytest.raises(
        docker.errors.ContainerError,
        match=f"{env_var_to_remove} env var or secret must be set.",
    ):
        out = docker_client.containers.run(
            f"flowminder/flowetl:{container_tag}",
            environment=env,
            stderr=True,
            user="airflow",
            remove=True,
        )
