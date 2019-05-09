"""
Conftest for flowetl integration tests
"""
import os
import shutil

from time import sleep
from subprocess import DEVNULL, Popen
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
    # breif sleep to wait for backing DB to be ready
    sleep(2)
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
        return out.decode("utf-8")

    return run_commmand


# @pytest.fixture(scope="module")
# def airflow_dagbag():
#     from airflow.models import DagBag

#     yield DagBag("./dags", include_examples=False)
#     shutil.rmtree(os.environ["AIRFLOW_HOME"])


@pytest.fixture(scope="function")
def airflow_local_setup():
    extra_env = {
        "AIRFLOW__CORE__DAGS_FOLDER": "./dags",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
    }
    env = {**os.environ, **extra_env}
    initdb = Popen(
        ["airflow", "initdb"], shell=False, stdout=DEVNULL, stderr=DEVNULL, env=env
    )
    initdb.wait()

    with open("scheduler.log", "w") as f:
        scheduler = Popen(
            ["airflow", "scheduler"], shell=False, stdout=f, stderr=f, env=env
        )

    sleep(2)

    with open("webserver.log", "w") as f:
        webserver = Popen(
            ["airflow", "webserver"], shell=False, stdout=f, stderr=f, env=env
        )

    sleep(2)
    # yeilding a lambda that allows for subprocess calls with the correct env
    yield lambda cmd, **kwargs: Popen(cmd.split(), env=env, **kwargs)
    scheduler.terminate()
    webserver.terminate()

    shutil.rmtree(env["AIRFLOW_HOME"])
    os.unlink("./scheduler.log")
    os.unlink("./webserver.log")
