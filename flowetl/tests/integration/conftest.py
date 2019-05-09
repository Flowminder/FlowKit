"""
Conftest for flowetl integration tests
"""

import pytest
import docker
import os


@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


@pytest.fixture(scope="session")
def flowetl_tag():
    return os.environ.get("FLOWETL_TAG", "latest")


@pytest.fixture(scope="module")
def flowetl_solo_longrunning(docker_client, flowetl_tag):
    container = docker_client.containers.run(
        f"flowminder/flowetl:{flowetl_tag}", detach=True
    )
    yield container
    container.kill()
    container.remove()


@pytest.fixture(scope="function")
def flowetl_run_command(docker_client, flowetl_tag):
    def run_commmand(command, **kwargs):
        out = docker_client.containers.run(
            f"flowminder/flowetl:{flowetl_tag}", command, **kwargs
        )
        return out

    return run_commmand

