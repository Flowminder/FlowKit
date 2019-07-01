# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Conftest for flowetl integration tests
"""
import docker
import os
import shutil
import structlog
import pytest
import requests
import json

from pathlib import Path
from time import sleep
from subprocess import DEVNULL, Popen
from pendulum import now, Interval
from airflow.models import DagRun
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from docker.types import Mount
from shutil import rmtree
from requests.exceptions import RequestException

here = os.path.dirname(os.path.abspath(__file__))
logger = structlog.get_logger("flowetl-tests")


@pytest.fixture(scope="session", autouse=True)
def ensure_required_env_vars_are_set():
    if "AIRFLOW_HOME" not in os.environ:
        raise RuntimeError(
            "Must set environment variable AIRFLOW_HOME to run the flowetl tests."
        )

    if "true" != os.getenv("TESTING", "false").lower():
        raise RuntimeError(
            "Must set environment variable TESTING='true' to run the flowetl tests."
        )

    if "FLOWETL_TESTS_CONTAINER_TAG" not in os.environ:
        raise RuntimeError(
            "Must explicitly set environment variable FLOWETL_TESTS_CONTAINER_TAG to run the flowetl tests. "
            "(Use `FLOWETL_TESTS_CONTAINER_TAG=latest` if you don't need a specific version.)"
        )


@pytest.fixture(scope="session")
def flowetl_mounts_dir():
    return os.path.abspath(os.path.join(here, "..", "..", "mounts"))


@pytest.fixture(scope="session")
def docker_client():
    """
    docker client object - used to run containers
    """
    return docker.from_env()


@pytest.fixture(scope="session")
def docker_api_client():
    """
    docker APIClient object - needed to inspect containers
    """
    return docker.APIClient()


@pytest.fixture(scope="session")
def container_tag():
    """
    Get tag to use for containers
    """
    return os.environ["FLOWETL_TESTS_CONTAINER_TAG"]


@pytest.fixture(scope="session")
def container_env():
    """
    Environments for each container
    """
    flowdb = {
        "POSTGRES_USER": "flowdb",
        "POSTGRES_PASSWORD": "flowflow",
        "POSTGRES_DB": "flowdb",
        "FLOWMACHINE_FLOWDB_USER": "flowmachine",
        "FLOWAPI_FLOWDB_USER": "flowapi",
        "FLOWMACHINE_FLOWDB_PASSWORD": "flowmachine",
        "FLOWAPI_FLOWDB_PASSWORD": "flowapi",
    }

    flowetl_db = {
        "POSTGRES_USER": "flowetl",
        "POSTGRES_PASSWORD": "flowetl",
        "POSTGRES_DB": "flowetl",
    }

    flowetl = {
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__CORE__FERNET_KEY": "ssgBqImdmQamCrM9jNhxI_IXSzvyVIfqvyzES67qqVU=",
        "AIRFLOW__CORE__SQL_ALCHEMY_CONN": f"postgres://{flowetl_db['POSTGRES_USER']}:{flowetl_db['POSTGRES_PASSWORD']}@flowetl_db:5432/{flowetl_db['POSTGRES_DB']}",
        "AIRFLOW_CONN_FLOWDB": f"postgres://{flowdb['POSTGRES_USER']}:{flowdb['POSTGRES_PASSWORD']}@flowdb:5432/flowdb",
        "POSTGRES_USER": "flowetl",
        "POSTGRES_PASSWORD": "flowetl",
        "POSTGRES_DB": "flowetl",
        "POSTGRES_HOST": "flowetl_db",
        "AIRFLOW__WEBSERVER__WEB_SERVER_HOST": "0.0.0.0",  # helpful for circle debugging
    }

    return {"flowetl": flowetl, "flowdb": flowdb, "flowetl_db": flowetl_db}


@pytest.fixture(scope="session")
def container_ports():
    """
    Exposed ports for flowetl_db and flowdb (
    """
    flowetl_airflow_host_port = 28080
    flowetl_db_host_port = 29000
    flowdb_host_port = 29001

    return {
        "flowetl_airflow": flowetl_airflow_host_port,
        "flowetl_db": flowetl_db_host_port,
        "flowdb": flowdb_host_port,
    }


@pytest.fixture(scope="function")
def container_network(docker_client):
    """
    A docker network for containers to communicate on
    """
    network = docker_client.networks.create("testing", driver="bridge")
    yield
    network.remove()


@pytest.fixture(scope="function")
def postgres_data_dir_for_tests():
    """
    Creates and cleans up a directory for storing pg data.
    Used by Flowdb because on unix changing flowdb user is
    incompatible with using a volume for the DB's data.
    """
    path = f"{os.getcwd()}/pg_data"
    if not os.path.exists(path):
        os.makedirs(path)
    yield path
    rmtree(path)


@pytest.fixture(scope="function")
def mounts(postgres_data_dir_for_tests, flowetl_mounts_dir):
    """
    Various mount objects needed by containers
    """
    config_mount = Mount("/mounts/config", f"{flowetl_mounts_dir}/config", type="bind")
    files_mount = Mount("/mounts/files", f"{flowetl_mounts_dir}/files", type="bind")
    flowetl_mounts = [config_mount, files_mount]

    data_mount = Mount(
        "/var/lib/postgresql/data", postgres_data_dir_for_tests, type="bind"
    )
    files_mount = Mount("/files", f"{flowetl_mounts_dir}/files", type="bind")
    flowdb_mounts = [data_mount, files_mount]

    return {"flowetl": flowetl_mounts, "flowdb": flowdb_mounts}


@pytest.fixture(scope="session", autouse=True)
def pull_docker_images(docker_client, container_tag):
    logger.info(f"Pulling docker images (tag='{container_tag}')...")
    docker_client.images.pull("postgres", tag="11.0")
    docker_client.images.pull("flowminder/flowdb", tag=container_tag)
    docker_client.images.pull("flowminder/flowetl", tag=container_tag)
    logger.info("Done pulling docker images.")


@pytest.fixture(scope="function")
def flowdb_container(
    docker_client,
    docker_api_client,
    container_tag,
    container_env,
    container_ports,
    container_network,
    mounts,
):
    """
    Starts flowdb (and cleans up) and waits until healthy
    - so that we can be sure connections to the DB will work.
    Setting user to uid/gid of user running the tests - necessary
    for ingestion.
    """
    user = f"{os.getuid()}:{os.getgid()}"
    container = docker_client.containers.run(
        f"flowminder/flowdb:{container_tag}",
        environment=container_env["flowdb"],
        ports={"5432": container_ports["flowdb"]},
        name="flowdb",
        network="testing",
        mounts=mounts["flowdb"],
        healthcheck={"test": "pg_isready -h localhost -U flowdb"},
        user=user,
        detach=True,
    )

    healthy = False
    while not healthy:
        container_info = docker_api_client.inspect_container(container.id)
        healthy = container_info["State"]["Health"]["Status"] == "healthy"

    yield
    container.kill()
    container.remove()


@pytest.fixture(scope="function")
def flowetl_db_container(
    docker_client, container_env, container_ports, container_network
):
    """
    Start (and clean up) flowetl_db just a vanilla pg 11
    """
    container = docker_client.containers.run(
        f"postgres:11.0",
        environment=container_env["flowetl_db"],
        ports={"5432": container_ports["flowetl_db"]},
        name="flowetl_db",
        network="testing",
        detach=True,
    )
    yield
    container.kill()
    container.remove()


@pytest.fixture(scope="function")
def flowetl_container(
    flowdb_container,
    flowetl_db_container,
    docker_client,
    container_tag,
    container_env,
    container_network,
    mounts,
    container_ports,
):
    """
    Start (and clean up) flowetl. Setting user to uid/gid of
    user running the tests - necessary for moving files between
    host directories.
    """

    def wait_for_container(
        *, time_out=Interval(minutes=3), time_out_per_request=Interval(seconds=1)
    ):
        # Tries to make constant requests to the health check endpoint for quite
        # arbitrarily 3 minutes.. Fails with a TimeoutError if it can't reach the
        # endpoint during the attempts.
        t0 = now()
        healthy = False
        while not healthy:
            try:
                resp = requests.get(
                    f"http://localhost:{container_ports['flowetl_airflow']}/health",
                    timeout=time_out_per_request.seconds,
                ).json()

                # retry if scheduler is still not healthy
                if resp["scheduler"]["status"] == "healthy":
                    healthy = True
            except RequestException:
                pass

            sleep(0.1)
            t1 = now()
            if (t1 - t0) > time_out:
                raise TimeoutError(
                    "Flowetl container did not start properly. This may be due to "
                    "missing config settings or syntax errors in one of its task."
                )

    user = f"{os.getuid()}:{os.getgid()}"
    container = docker_client.containers.run(
        f"flowminder/flowetl:{container_tag}",
        environment=container_env["flowetl"],
        name="flowetl",
        network="testing",
        restart_policy={"Name": "always"},
        ports={"8080": container_ports["flowetl_airflow"]},
        mounts=mounts["flowetl"],
        user=user,
        detach=True,
    )
    wait_for_container()
    yield container
    container.kill()
    container.remove()


@pytest.fixture(scope="function")
def trigger_dags():
    """
    Returns a function that unpauses all DAGs and then triggers
    the etl_sensor DAG.
    """

    def trigger_dags_function(*, flowetl_container):

        dags = ["etl_sensor", "etl_sms", "etl_mds", "etl_calls", "etl_topups"]

        for dag in dags:
            flowetl_container.exec_run(f"airflow unpause {dag}")

        flowetl_container.exec_run("airflow trigger_dag etl_sensor")

    return trigger_dags_function


@pytest.fixture(scope="function")
def write_files_to_files(flowetl_mounts_dir):
    """
    Returns a function that allows for writing a list
    of empty files to the files location. Also cleans
    up the files location.
    """
    files_dir = f"{flowetl_mounts_dir}/files"

    def write_files_to_files_function(*, file_names):
        for file_name in file_names:
            Path(f"{files_dir}/{file_name}").touch()

    yield write_files_to_files_function

    files_to_remove = Path(files_dir).glob("*")
    files_to_remove = filter(lambda file: file.name != "README.md", files_to_remove)

    [file.unlink() for file in files_to_remove]


@pytest.fixture(scope="module")
def airflow_local_setup():
    """
    Init the airflow sqlitedb and start the scheduler with minimal env.
    Clean up afterwards by removing the AIRFLOW_HOME and stopping the
    scheduler. It is necessary to set the AIRFLOW_HOME env variable on
    test invocation otherwise it gets created somewhere else.
    """
    extra_env = {
        "AIRFLOW__CORE__DAGS_FOLDER": "./dags",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
    }
    env = {**os.environ, **extra_env}
    # make test Airflow home dir
    airflow_home_dir_for_tests = os.environ["AIRFLOW_HOME"]
    if not os.path.exists(airflow_home_dir_for_tests):
        os.makedirs(airflow_home_dir_for_tests)

    initdb = Popen(
        ["airflow", "initdb"], shell=False, stdout=DEVNULL, stderr=DEVNULL, env=env
    )
    initdb.wait()

    with open("scheduler.log", "w") as fout:
        scheduler = Popen(
            ["airflow", "scheduler"], shell=False, stdout=fout, stderr=fout, env=env
        )

    sleep(2)

    yield

    scheduler.terminate()

    shutil.rmtree(airflow_home_dir_for_tests)
    os.unlink("./scheduler.log")


@pytest.fixture(scope="function")
def airflow_local_pipeline_run():
    """
    As in `airflow_local_setup` but starts the scheduler with some extra env
    determined in the test. Also triggers the etl_sensor dag causing a
    subsequent trigger of the etl dag.
    """
    scheduler_to_clean_up = None
    airflow_home_dir_for_tests = None

    def run_func(extra_env):
        nonlocal scheduler_to_clean_up
        nonlocal airflow_home_dir_for_tests
        default_env = {
            "AIRFLOW__CORE__DAGS_FOLDER": "./dags",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        }
        env = {**os.environ, **default_env, **extra_env}

        # make test Airflow home dir
        airflow_home_dir_for_tests = os.environ["AIRFLOW_HOME"]
        if not os.path.exists(airflow_home_dir_for_tests):
            os.makedirs(airflow_home_dir_for_tests)

        initdb = Popen(
            ["airflow", "initdb"], shell=False, stdout=DEVNULL, stderr=DEVNULL, env=env
        )
        initdb.wait()

        with open("scheduler.log", "w") as fout:
            scheduler = Popen(
                ["airflow", "scheduler"], shell=False, stdout=fout, stderr=fout, env=env
            )
            scheduler_to_clean_up = scheduler

        sleep(2)

        p = Popen(
            "airflow unpause etl_sensor".split(),
            shell=False,
            stdout=DEVNULL,
            stderr=DEVNULL,
            env=env,
        )
        p.wait()

        p = Popen(
            "airflow unpause etl_testing".split(),
            shell=False,
            stdout=DEVNULL,
            stderr=DEVNULL,
            env=env,
        )
        p.wait()

        p = Popen(
            "airflow trigger_dag etl_sensor".split(),
            shell=False,
            stdout=DEVNULL,
            stderr=DEVNULL,
            env=env,
        )
        p.wait()

    yield run_func

    scheduler_to_clean_up.terminate()

    shutil.rmtree(airflow_home_dir_for_tests)
    os.unlink("./scheduler.log")


@pytest.fixture(scope="function")
def wait_for_completion():
    """
    Return a function that waits for the etl dag to be in a specific
    end state. If dag does not reach this state within (arbitrarily but
    seems OK...) three minutes raises a TimeoutError.
    """

    def wait_func(end_state, dag_id, session=None, time_out=Interval(minutes=3)):
        # if you actually pass None to DagRun.find it thinks None is the session
        # you want to use - need to not pass at all if you want airflow to pick
        # up correct session using it's provide_session decorator...
        if session is None:
            kwargs = {"dag_id": dag_id, "state": end_state}
        else:
            kwargs = {"dag_id": dag_id, "state": end_state, "session": session}

        t0 = now()
        while len(DagRun.find(**kwargs)) != 1:
            sleep(1)
            t1 = now()
            if (t1 - t0) > time_out:
                raise TimeoutError(
                    f"DAG '{dag_id}' did not reach desired state {end_state}. This may be"
                    "due to missing config settings or syntax errors in one of its task."
                )
        return end_state

    return wait_func


@pytest.fixture(scope="function")
def flowdb_connection_engine(container_env, container_ports):
    """
    Engine for flowdb
    """
    conn_str = f"postgresql://{container_env['flowdb']['POSTGRES_USER']}:{container_env['flowdb']['POSTGRES_PASSWORD']}@localhost:{container_ports['flowdb']}/flowdb"
    engine = create_engine(conn_str)

    return engine


@pytest.fixture(scope="function")
def flowdb_connection(flowdb_connection_engine):
    """
    Connection for flowdb - allowing for execution of
    raw sql.
    """
    connection = flowdb_connection_engine.connect()
    trans = connection.begin()
    yield connection, trans
    connection.close()


@pytest.fixture(scope="function")
def flowdb_session(flowdb_connection_engine):
    """
    sqlalchmy session for flowdb - used for ORM models
    """
    session = sessionmaker(bind=flowdb_connection_engine)()
    yield session
    session.close()


@pytest.fixture(scope="function")
def flowetl_db_connection_engine(container_env, container_ports):
    """
    Engine for flowetl_db
    """
    conn_str = f"postgresql://{container_env['flowetl_db']['POSTGRES_USER']}:{container_env['flowetl_db']['POSTGRES_PASSWORD']}@localhost:{container_ports['flowetl_db']}/{container_env['flowetl_db']['POSTGRES_DB']}"
    logger.info(conn_str)
    engine = create_engine(conn_str)

    return engine


@pytest.fixture(scope="function")
def flowetl_db_session(flowetl_db_connection_engine):
    """
    sqlalchmy session for flowetl - used for ORM models
    """
    session = sessionmaker(bind=flowetl_db_connection_engine)()
    yield session
    session.close()
