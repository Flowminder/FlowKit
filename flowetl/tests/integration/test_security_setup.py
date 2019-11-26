# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import docker
import pytest


@pytest.mark.parametrize(
    "env_var_to_remove",
    ["FLOWETL_AIRFLOW_ADMIN_USERNAME", "FLOWETL_AIRFLOW_ADMIN_PASSWORD"],
)
def test_required_env_var(env_var_to_remove, docker_client, container_tag):
    """
    Test that an error is raised when one of the required env vars is not set.
    """
    env = {
        "FLOWETL_AIRFLOW_ADMIN_USERNAME": "admin",
        "FLOWETL_AIRFLOW_ADMIN_PASSWORD": "password",
    }
    env.pop(env_var_to_remove)
    with pytest.raises(
        docker.errors.ContainerError, match=f"Need to set {env_var_to_remove} non-empty"
    ):
        out = docker_client.containers.run(
            f"flowminder/flowetl:{container_tag}", environment=env,
        )
