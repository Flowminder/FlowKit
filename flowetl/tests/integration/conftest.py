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
import logging
import pytest

from itertools import chain
from pathlib import Path
from time import sleep
from subprocess import DEVNULL, Popen
from pendulum import now, Interval
from airflow.models import DagRun
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from docker.types import Mount
from shutil import rmtree


@pytest.fixture(scope="session")
def docker_client():
    """
    docker client object - used to run containers
    """
    return docker.from_env()


@pytest.fixture(scope="session")
def docker_APIClient():
    """
    docker APIClient object - needed to inspect containers
    """
    return docker.APIClient()


@pytest.fixture(scope="session")
def tag():
    """
    Get tag to use for containers
    """
    return os.environ.get("FLOWETL_CONTAINERS_TAG", "latest")


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
        "MOUNT_HOME": "/mounts",
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
    flowetl_airflow_host_port = 8080
    flowetl_db_host_port = 9000
    flowdb_host_port = 9001

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
def data_dir():
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
def mounts(data_dir):
    """
    Various mount objects needed by containers
    """
    config_mount = Mount("/mounts/config", f"{os.getcwd()}/mounts/config", type="bind")
    archive_mount = Mount(
        "/mounts/archive", f"{os.getcwd()}/mounts/archive", type="bind"
    )
    dump_mount = Mount("/mounts/dump", f"{os.getcwd()}/mounts/dump", type="bind")
    ingest_mount = Mount("/mounts/ingest", f"{os.getcwd()}/mounts/ingest", type="bind")
    quarantine_mount = Mount(
        "/mounts/quarantine", f"{os.getcwd()}/mounts/quarantine", type="bind"
    )
    flowetl_mounts = [
        config_mount,
        archive_mount,
        dump_mount,
        ingest_mount,
        quarantine_mount,
    ]

    data_mount = Mount("/var/lib/postgresql/data", data_dir, type="bind")
    ingest_mount = Mount("/ingest", f"{os.getcwd()}/mounts/ingest", type="bind")
    flowdb_mounts = [data_mount, ingest_mount]

    return {"flowetl": flowetl_mounts, "flowdb": flowdb_mounts}


@pytest.fixture(scope="function")
def flowdb_container(
    docker_client,
    docker_APIClient,
    tag,
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
        f"flowminder/flowdb:{tag}",
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
        container_info = docker_APIClient.inspect_container(container.id)
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
    tag,
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
    user = f"{os.getuid()}:{os.getgid()}"
    container = docker_client.containers.run(
        f"flowminder/flowetl:{tag}",
        environment=container_env["flowetl"],
        name="flowetl",
        network="testing",
        restart_policy={"Name": "always"},
        ports={"8080": container_ports["flowetl_airflow"]},
        mounts=mounts["flowetl"],
        user=user,
        detach=True,
    )
    sleep(10)  # BADDD but no clear way to know that airflow scheduler is ready!
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
def write_files_to_dump():
    """
    Returns a function that allows for writing a list
    of empty files to the dump location. Also cleans
    up dump, archive and quarantine.
    """
    dump_dir = f"{os.getcwd()}/mounts/dump"
    archive_dir = f"{os.getcwd()}/mounts/archive"
    quarantine_dir = f"{os.getcwd()}/mounts/quarantine"

    def write_files_to_dump_function(*, file_names):
        for file_name in file_names:
            Path(f"{dump_dir}/{file_name}").touch()

    yield write_files_to_dump_function

    files_to_remove = chain(
        Path(dump_dir).glob("*"),
        Path(archive_dir).glob("*"),
        Path(quarantine_dir).glob("*"),
    )
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
    test_airflow_home_dir = os.environ["AIRFLOW_HOME"]
    if not os.path.exists(test_airflow_home_dir):
        os.makedirs(test_airflow_home_dir)

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

    shutil.rmtree(test_airflow_home_dir)
    os.unlink("./scheduler.log")


@pytest.fixture(scope="function")
def airflow_local_pipeline_run():
    """
    As in `airflow_local_setup` but starts the scheduler with some extra env
    determined in the test. Also triggers the etl_sensor dag causing a
    subsequent trigger of the etl dag.
    """
    scheduler_to_clean_up = None
    test_airflow_home_dir = None

    def run_func(extra_env):
        nonlocal scheduler_to_clean_up
        nonlocal test_airflow_home_dir
        default_env = {
            "AIRFLOW__CORE__DAGS_FOLDER": "./dags",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        }
        env = {**os.environ, **default_env, **extra_env}

        # make test Airflow home dir
        test_airflow_home_dir = os.environ["AIRFLOW_HOME"]
        if not os.path.exists(test_airflow_home_dir):
            os.makedirs(test_airflow_home_dir)

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

    yield run_func

    scheduler_to_clean_up.terminate()

    shutil.rmtree(test_airflow_home_dir)
    os.unlink("./scheduler.log")


@pytest.fixture(scope="function")
def wait_for_completion():
    """
    Return a function that waits for the etl dag to be in a specific
    end state. If dag does not reach this state within (arbitrarily but
    seems OK...) three minutes raises a TimeoutError.
    """

    def wait_func(
        end_state, dag_id, session=None, count=1, time_out=Interval(minutes=3)
    ):
        # if you actually pass None to DagRun.find it thinks None is the session
        # you want to use - need to not pass at all if you want airflow to pick
        # up correct session using it's provide_session decorator...
        if session is None:
            t0 = now()
            while len(DagRun.find(dag_id, state=end_state)) != count:
                sleep(1)
                t1 = now()
                if (t1 - t0) > time_out:
                    raise TimeoutError
            return end_state
        else:
            t0 = now()
            while len(DagRun.find(dag_id, state=end_state, session=session)) != count:
                sleep(1)
                t1 = now()
                if (t1 - t0) > time_out:
                    raise TimeoutError
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
    logging.info(conn_str)
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
