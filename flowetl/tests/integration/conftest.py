# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Conftest for flowetl integration tests
"""
import warnings

import docker
import os
import structlog
import pytest
import requests

from pathlib import Path


from time import sleep
from subprocess import Popen, run
from pendulum import now, Interval
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from docker.types import Mount
from requests.exceptions import RequestException

here = os.path.dirname(os.path.abspath(__file__))
logger = structlog.get_logger("flowetl-tests")


@pytest.fixture
def ensure_required_env_vars_are_set(monkeypatch):

    monkeypatch.setenv("FLOWETL_RUNTIME_CONFIG", "testing")

    if "FLOWETL_TESTS_CONTAINER_TAG" not in os.environ:
        monkeypatch.setenv("FLOWETL_TESTS_CONTAINER_TAG", "latest")
        warnings.warn(
            "You should explicitly set environment variable FLOWETL_TESTS_CONTAINER_TAG to run the flowetl tests. Using 'latest'."
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


@pytest.fixture
def container_tag(ensure_required_env_vars_are_set):
    """
    Get tag to use for containers
    """
    return os.environ["FLOWETL_TESTS_CONTAINER_TAG"]


@pytest.fixture
def container_env(ensure_required_env_vars_are_set):
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
        "AIRFLOW__WEBSERVER__WEB_SERVER_HOST": "0.0.0.0",  # helpful for circle debugging,
        "FLOWETL_AIRFLOW_ADMIN_USERNAME": "admin",
        "FLOWETL_AIRFLOW_ADMIN_PASSWORD": "password",
        "AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC": 10,
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
def mounts(tmpdir, flowetl_mounts_dir):
    """
    Various mount objects needed by containers
    """
    config_mount = Mount("/mounts/config", f"{flowetl_mounts_dir}/config", type="bind")
    files_mount = Mount("/mounts/files", f"{flowetl_mounts_dir}/files", type="bind")
    logs_mount = Mount("/mounts/logs", f"{flowetl_mounts_dir}/logs", type="bind")
    flowetl_mounts = [config_mount, files_mount, logs_mount]

    data_mount = Mount("/var/lib/postgresql/data", str(tmpdir), type="bind")
    files_mount = Mount("/files", f"{flowetl_mounts_dir}/files", type="bind")
    flowdb_mounts = [data_mount, files_mount]

    return {"flowetl": flowetl_mounts, "flowdb": flowdb_mounts}


@pytest.fixture
def pull_docker_images(docker_client, container_tag):
    disable_pulling_docker_images = (
        os.getenv(
            "FLOWETL_INTEGRATION_TESTS_DISABLE_PULLING_DOCKER_IMAGES", "FALSE"
        ).upper()
        == "TRUE"
    )
    if disable_pulling_docker_images:
        logger.info(
            "Not pulling docker images for FlowETL integration tests "
            "(because FLOWETL_INTEGRATION_TESTS_DISABLE_PULLING_DOCKER_IMAGES=TRUE)."
        )
    else:
        logger.info(f"Pulling docker images (tag='{container_tag}')...")
        docker_client.images.pull("postgres", tag="11.0")
        docker_client.images.pull("flowminder/flowdb", tag=container_tag)
        docker_client.images.pull("flowminder/flowetl", tag=container_tag)
        logger.info("Done pulling docker images.")


@pytest.fixture(scope="function")
def flowdb_container(
    pull_docker_images,
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

    # Add a single line of raw SMS data into a postgres table which is used in the full-pipeline test.
    engine = create_engine(
        f"postgresql://flowdb:flowflow@localhost:{container_ports['flowdb']}/flowdb"
    )
    engine.execute(
        """
        CREATE TABLE IF NOT EXISTS mds_raw_data_dump (
            imei TEXT,
            msisdn TEXT,
            event_time TIMESTAMPTZ,
            cell_id TEXT
        );

        INSERT INTO mds_raw_data_dump VALUES
        ('BDED3095A2759089134DDA5CB7968764', '9824B87CDEEAD5ED5AC959D74F3C81C5', '2016-01-01 13:23:29', 'C44BEF');
        """
    )

    yield
    container.kill()
    container.remove()


@pytest.fixture(scope="function")
def flowetl_db_container(
    pull_docker_images, docker_client, container_env, container_ports, container_network
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
        healthcheck={
            "test": f"pg_isready -h 127.0.0.1 -p 5432 -U {container_env['flowetl_db']['POSTGRES_USER']})"
        },
        detach=True,
    )
    # Wait for container to be ready
    healthy = False
    while not healthy:
        container_info = docker_api_client.inspect_container(container.id)
        healthy = container_info["State"]["Health"]["Status"] == "healthy"

    yield
    container.kill()
    container.remove()


@pytest.fixture(scope="function")
def flowetl_container(
    pull_docker_images,
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
        stderr=True,
    )
    wait_for_container()
    yield container

    save_airflow_logs = (
        os.getenv("FLOWETL_INTEGRATION_TESTS_SAVE_AIRFLOW_LOGS", "FALSE").upper()
        == "TRUE"
    )
    if save_airflow_logs:
        logger.info(
            "Saving airflow logs to /mounts/logs/ and outputting to stdout "
            "(because FLOWETL_INTEGRATION_TESTS_SAVE_AIRFLOW_LOGS=TRUE)."
        )
        container.exec_run("bash -c 'cp -r $AIRFLOW_HOME/logs/* /mounts/logs/'")
        airflow_logs = container.exec_run(
            "bash -c 'find /mounts/logs -type f -exec cat {} \;'"
        )
        logger.info(airflow_logs)

    container.kill()
    container.remove()


@pytest.fixture(scope="function")
def trigger_dags(flowetl_container):
    """
    Returns a function that unpauses all DAGs and then triggers
    the etl_sensor DAG.
    """

    def trigger_dags_function():

        dags = ["etl_sensor", "etl_sms", "etl_mds", "etl_calls", "etl_topups"]

        for dag in dags:
            tries = 0
            while True:
                exit_code, result = flowetl_container.exec_run(f"airflow unpause {dag}")
                logger.info(f"Triggered: {dag}. {result}")
                if exit_code == 1:
                    break
                if tries > 10:
                    Exception(f"Failed to unpause {dag}: {result}")
                tries += 1

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


@pytest.fixture
def airflow_local_setup(airflow_home):
    """
    Init the airflow sqlitedb and start the scheduler with minimal env.
    Clean up afterwards by removing the AIRFLOW_HOME and stopping the
    scheduler. It is necessary to set the AIRFLOW_HOME env variable on
    test invocation otherwise it gets created somewhere else.
    """
    extra_env = {
        "AIRFLOW__CORE__DAGS_FOLDER": str(Path(__file__).parent.parent.parent / "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
    }
    env = {**os.environ, **extra_env}

    from airflow.bin.cli import initdb

    initdb([])

    with open(airflow_home / "scheduler.log", "w") as fout:
        with Popen(
            ["airflow", "scheduler"], shell=False, stdout=fout, stderr=fout, env=env
        ) as scheduler:

            sleep(2)

            yield


@pytest.fixture
def airflow_home(tmpdir, monkeypatch, ensure_required_env_vars_are_set):
    logger.info(f"AIRFLOW_HOME={tmpdir}")
    monkeypatch.setenv("AIRFLOW_HOME", str(tmpdir))
    yield tmpdir


@pytest.fixture(
    params=[
        (
            "init",
            {
                "init": "failed",
                "extract": "upstream_failed",
                "transform": "upstream_failed",
                "success_branch": "success",
                "load": "upstream_failed",
                "postload": "skipped",
                "archive": "skipped",
                "quarantine": "success",
                "clean": "success",
                "fail": "failed",
            },
        ),
        # (
        #     "extract",
        #     {
        #         "init": "success",
        #         "extract": "failed",
        #         "transform": "upstream_failed",
        #         "success_branch": "success",
        #         "load": "upstream_failed",
        #         "archive": "skipped",
        #         "quarantine": "success",
        #         "clean": "success",
        #         "fail": "failed",
        #     },
        # ),
        # (
        #     "transform",
        #     {
        #         "init": "success",
        #         "extract": "success",
        #         "transform": "failed",
        #         "success_branch": "success",
        #         "load": "upstream_failed",
        #         "archive": "skipped",
        #         "quarantine": "success",
        #         "clean": "success",
        #         "fail": "failed",
        #     },
        # ),
        # (
        #     "load",
        #     {
        #         "init": "success",
        #         "extract": "success",
        #         "transform": "success",
        #         "success_branch": "success",
        #         "load": "failed",
        #         "archive": "skipped",
        #         "quarantine": "success",
        #         "clean": "success",
        #         "fail": "failed",
        #     },
        # ),
    ]
)
def airflow_local_pipeline_run(airflow_home, request):
    """
    As in `airflow_local_setup` but starts the scheduler with some extra env
    determined in the test. Also triggers the etl_sensor dag causing a
    subsequent trigger of the etl dag.
    """
    task_to_fail, expected_task_states = request.param
    default_env = {
        "AIRFLOW__CORE__DAGS_FOLDER": str(Path(__file__).parent.parent.parent / "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
    }
    env = {**os.environ, **default_env, **{"TASK_TO_FAIL": task_to_fail}}

    def run_func():
        tries = 0
        while True:
            p = run("airflow unpause etl_sensor".split(), capture_output=True, env=env)
            if p.returncode == 0:
                break
            if tries > 20:
                raise Exception(f"Couldn't unpause: {p.stderr}")
            tries += 1
            logger.info(f"Unpause failed. Retrying. {p.stderr}")
            sleep(0.5)

        p = run("airflow unpause etl_testing".split(), capture_output=True, env=env,)

        p = run("airflow trigger_dag etl_sensor".split(), capture_output=True, env=env,)

    p = run("airflow initdb".split(), capture_output=True, env=env)

    with open(airflow_home / "scheduler.log", "w") as fout:
        logger.info("Starting scheduler.")
        with Popen(
            ["airflow", "scheduler"], shell=False, stdout=fout, stderr=fout, env=env,
        ) as scheduler:
            sleep(10)
            logger.info("Yielding run func.")
            yield run_func, expected_task_states
            scheduler.kill()
    logger.info("Stopped scheduler.")


@pytest.fixture(scope="function")
def wait_for_completion(airflow_home):
    """
    Return a function that waits for the etl dag to be in a specific
    end state. If dag does not reach this state within (arbitrarily but
    seems OK...) three minutes raises a TimeoutError.
    """

    def wait_func(
        end_state, fail_state, dag_id, session=None, time_out=Interval(minutes=5)
    ):
        # if you actually pass None to DagRun.find it thinks None is the session
        # you want to use - need to not pass at all if you want airflow to pick
        # up correct session using it's provide_session decorator...
        if session is None:
            kwargs_expected = {"dag_id": dag_id, "state": end_state}
            kwargs_fail = {"dag_id": dag_id, "state": fail_state}
        else:
            kwargs_expected = {"dag_id": dag_id, "state": end_state, "session": session}
            kwargs_fail = {"dag_id": dag_id, "state": fail_state, "session": session}

        t0 = now()
        from airflow.models import DagRun

        logger.info(os.environ["AIRFLOW_HOME"])
        while len(DagRun.find(**kwargs_expected)) != 1:
            sleep(1)
            t1 = now()
            if (t1 - t0) > time_out or len(DagRun.find(**kwargs_fail)) == 1:
                raise TimeoutError(
                    f"DAG '{dag_id}' did not reach desired state {end_state}. This may be "
                    "due to missing config settings, syntax errors in one of its task, or "
                    "similar runtime errors."
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
    sqlalchemy session for flowdb - used for ORM models
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
    sqlalchemy session for flowetl - used for ORM models
    """
    session = sessionmaker(bind=flowetl_db_connection_engine)()
    yield session
    session.close()
