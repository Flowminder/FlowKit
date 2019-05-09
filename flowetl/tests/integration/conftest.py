"""
Conftest for flowetl integration tests
"""
import os

import pytest
import docker


@pytest.fixture(scope="session")
def docker_client():
    """
    docker client object
    """
    return docker.from_env()


@pytest.fixture(scope="session")
def flowetl_tag():
    """
    Get flowetl tag to use
    """
    return os.environ.get("FLOWETL_TAG", "latest")


@pytest.fixture(scope="module")
def flowetl_solo_longrunning(docker_client, flowetl_tag):
    """
    Fixture that starts a running flowetl container and
    yeilds the container object.
    """
    container = docker_client.containers.run(
        f"flowminder/flowetl:{flowetl_tag}", detach=True
    )
    yield container
    container.kill()
    container.remove()


@pytest.fixture(scope="function")
def flowetl_run_command(docker_client, flowetl_tag):
    """
    Fixture that returns a function for running arbitrary
    command on flowetl container.
    """

    def run_commmand(command, **kwargs):
        out = docker_client.containers.run(
            f"flowminder/flowetl:{flowetl_tag}", command, **kwargs
        )
        return out

    return run_commmand
