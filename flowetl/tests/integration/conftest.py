# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Conftest for flowetl integration tests
"""
import os
import random
import string
import warnings
from pathlib import Path
from subprocess import Popen, run
from threading import Event, Thread
from time import sleep

import docker
import jinja2
import requests
import structlog
from docker.types import Mount
from requests.exceptions import RequestException

import pytest
from pendulum import duration, now
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

here = Path(__file__).parent
logger = structlog.get_logger("flowetl-tests")


@pytest.fixture(scope="session")
def monkeypatch_session(request):
    """Experimental (https://github.com/pytest-dev/pytest/issues/363)."""
    from _pytest.monkeypatch import MonkeyPatch

    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope="session")
def container_name_suffix():
    return f"flowetl_test_{''.join(random.choice(string.ascii_letters + string.digits) for n in range(32))}"


@pytest.fixture(scope="session")
def ensure_required_env_vars_are_set(monkeypatch_session):

    monkeypatch_session.setenv("FLOWETL_RUNTIME_CONFIG", "testing")

    if "FLOWETL_TESTS_CONTAINER_TAG" not in os.environ:
        monkeypatch_session.setenv("FLOWETL_TESTS_CONTAINER_TAG", "latest")
        warnings.warn(
            "You should explicitly set environment variable FLOWETL_TESTS_CONTAINER_TAG to run the flowetl tests. Using 'latest'."
        )
    logger.info(
        "Set env vars.", container_tag=os.environ["FLOWETL_TESTS_CONTAINER_TAG"]
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
def container_tag(ensure_required_env_vars_are_set):
    """
    Get tag to use for containers
    """
    return os.environ["FLOWETL_TESTS_CONTAINER_TAG"]


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def container_network(docker_client, container_name_suffix):
    """
    A docker network for containers to communicate on
    """
    network = docker_client.networks.create(
        f"testing_{container_name_suffix}", driver="bridge"
    )
    yield network
    for container in network.containers:
        try:
            network.disconnect(container=container, force=True)
        except docker.errors.APIError:
            pass  # Probably already disconnected
    network.remove()


@pytest.fixture(scope="session")
def mounts(tmpdir_factory, flowetl_mounts_dir):
    """
    Various mount objects needed by containers
    """
    logs = tmpdir_factory.mktemp("logs")
    pgdata = tmpdir_factory.mktemp("pgdata")

    dags_mount = Mount("/opt/airflow/dags", f"{flowetl_mounts_dir}/dags", type="bind")

    logs_mount = Mount("/mounts/logs", str(logs), type="bind")
    flowetl_mounts = [dags_mount, logs_mount]

    data_mount = Mount(
        "/var/lib/postgresql/data", str(pgdata), type="bind", consistency="delegated"
    )
    files_mount = Mount(
        "/files",
        str(Path(__file__).parent.parent.parent / "mounts" / "files"),
        type="bind",
    )
    flowdb_mounts = [data_mount, files_mount]

    return {"flowetl": flowetl_mounts, "flowdb": flowdb_mounts}


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def flowdb_container(
    pull_docker_images,
    docker_client,
    docker_api_client,
    container_tag,
    container_env,
    container_ports,
    container_network,
    mounts,
    container_name_suffix,
):
    """
    Starts flowdb (and cleans up) and waits until healthy
    - so that we can be sure connections to the DB will work.
    Setting user to uid/gid of user running the tests - necessary
    for ingestion.
    """
    user = f"{os.getuid()}:{os.getgid()}"
    logger.info("Starting FlowDB")
    container = docker_client.containers.run(
        f"flowminder/flowdb:{container_tag}",
        environment=container_env["flowdb"],
        ports={"5432": container_ports["flowdb"]},
        name=f"flowdb_{container_name_suffix}",
        mounts=mounts["flowdb"],
        healthcheck={
            "test": "pg_isready -h localhost -U flowdb",
            "interval": 100000000,
        },
        user=user,
        detach=True,
    )
    container_network.connect(container, aliases=["flowdb"])

    def await_container():
        # Wait for container to be ready
        healthy = False
        while not healthy:
            container_info = docker_api_client.inspect_container(container.id)
            healthy = container_info["State"]["Health"]["Status"] == "healthy"
        logger.info("Started FlowDB container")

    yield await_container

    engine = create_engine(
        f"postgresql://flowdb:flowflow@localhost:{container_ports['flowdb']}/flowdb"
    )
    engine.execute(
        """
        DROP TABLE IF EXISTS mds_raw_data_dump;
        """
    )
    container.kill()
    container.remove()


@pytest.fixture(scope="session")
def flowetl_db_container(
    pull_docker_images,
    docker_client,
    docker_api_client,
    container_env,
    container_ports,
    container_network,
    container_name_suffix,
):
    """
    Start (and clean up) flowetl_db just a vanilla pg 11
    """
    logger.info("Starting FlowETL db container")
    container = docker_client.containers.run(
        f"postgres:11.0",
        environment=container_env["flowetl_db"],
        ports={"5432": container_ports["flowetl_db"]},
        name=f"flowetl_db_{container_name_suffix}",
        healthcheck={
            "test": f"pg_isready -h localhost -p 5432 -U {container_env['flowetl_db']['POSTGRES_USER']}",
            "interval": 1000000000,
        },
        detach=True,
    )
    container_network.connect(container, aliases=["flowetl_db"])

    def await_container():
        # Wait for container to be ready
        healthy = False
        while not healthy:
            container_info = docker_api_client.inspect_container(container.id)
            healthy = container_info["State"]["Health"]["Status"] == "healthy"
        logger.info("Started FlowETL db container")

    yield await_container
    container.kill()
    container.remove()


@pytest.fixture(scope="session")
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
    container_name_suffix,
):
    """
    Start (and clean up) flowetl. Setting user to uid/gid of
    user running the tests - necessary for moving files between
    host directories.
    """

    def wait_for_container(
        *, time_out=duration(minutes=3), time_out_per_request=duration(seconds=1)
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

    user = f"{os.getuid()}:0"
    logger.info("Starting FlowETL container")
    container = docker_client.containers.run(
        f"flowminder/flowetl:{container_tag}",
        environment=container_env["flowetl"],
        name=f"flowetl_{container_name_suffix}",
        restart_policy={"Name": "always"},
        ports={"8080": container_ports["flowetl_airflow"]},
        mounts=mounts["flowetl"],
        user=user,
        detach=True,
        stderr=True,
    )
    container_network.connect(container, aliases=["flowetl"])
    try:
        wait_for_container()
        flowdb_container()
        flowetl_db_container()
        container.exec_run(
            "bash -c /init.sh",
            user=user,
        )
        logger.info("Started FlowETL container")
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
            container.exec_run(
                "bash -c 'cp -r $AIRFLOW_HOME/logs/* /mounts/logs/'", user="airflow"
            )
            airflow_logs = container.exec_run(
                "bash -c 'find /mounts/logs -type f -exec cat {} \;'"
            )
            logger.info(airflow_logs)
    except TimeoutError as exc:
        raise TimeoutError(
            f"Flowetl container did not start properly. This may be due to missing config settings or syntax errors in one of its task. Logs: {container.logs()}"
        )
    finally:
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
                exit_code, result = flowetl_container.exec_run(
                    f"airflow unpause {dag}", user="airflow"
                )
                logger.info(f"Triggered: {dag}. {result}")
                if exit_code == 0:
                    break
                if tries > 10:
                    Exception(f"Failed to unpause {dag}: {result}")
                tries += 1

        flowetl_container.exec_run("airflow trigger_dag etl_sensor", user="airflow")

    return trigger_dags_function


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

    from airflow.utils.db import initdb

    initdb()

    with open(airflow_home / "scheduler.log", "w") as fout:
        with Popen(
            ["pipenv", "run", "airflow", "scheduler"],
            shell=False,
            stdout=fout,
            stderr=fout,
            env=env,
        ) as scheduler:

            sleep(2)

            yield
            scheduler.kill()


@pytest.fixture
def airflow_home(tmpdir, monkeypatch, ensure_required_env_vars_are_set):
    logger.info(f"AIRFLOW_HOME={tmpdir}")
    monkeypatch.setenv("AIRFLOW_HOME", str(tmpdir))
    yield tmpdir


@pytest.fixture(scope="function")
def flowdb_connection_engine(container_env, container_ports, flowdb_container):
    """
    Engine for flowdb
    """
    flowdb_container()
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


@pytest.fixture
def jinja_env():
    import flowetl.qa_checks

    loader = jinja2.FileSystemLoader(
        searchpath=str(Path(list(flowetl.qa_checks.__path__)[0]) / "qa_checks")
    )
    yield jinja2.Environment(loader=loader)


@pytest.fixture
def flowdb_transaction(flowdb_connection):
    connection, trans = flowdb_connection
    yield connection
    trans.rollback()


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


@pytest.fixture
def run_task(flowetl_container):
    """
    Produces a function that will run a single task within a dag at a given execution date on the airflow container.

    Yields
    ------
    function
    """

    yield lambda dag_id, task_id, exec_date: flowetl_container.exec_run(
        f"airflow test {dag_id} {task_id} {exec_date}", user="airflow"
    )


@pytest.fixture
def run_dag(flowetl_container):
    """
    Produces a function that will run a single  dag at a given execution date on the airflow container.

    Yields
    ------
    function
    """

    def trigger_dag(*, dag_id, exec_date, run_id=None):
        trigger_cmd = ["airflow", "trigger_dag", "-e", exec_date]
        if run_id is not None:
            trigger_cmd += ["-r", run_id]

        flowetl_container.exec_run(["airflow", "unpause", dag_id], user="airflow")
        return flowetl_container.exec_run([*trigger_cmd, dag_id], user="airflow")

    yield trigger_dag


@pytest.fixture
def dag_status(flowetl_container):
    """
    Produces a function that will check the status of a dag.

    Yields
    ------
    function
    """

    def dag_status(*, dag_id, exec_date):
        status_cmd = ["airflow", "dag_state", dag_id, exec_date]
        return flowetl_container.exec_run(status_cmd, user="airflow")

    yield dag_status


@pytest.fixture
def task_status(flowetl_container):
    """
    Produces a function that will check the status of a task in a dag.

    Yields
    ------
    function
    """

    def task_status(*, dag_id, task_id, exec_date):
        status_cmd = ["airflow", "task_state", dag_id, task_id, exec_date]
        return flowetl_container.exec_run(status_cmd, user="airflow")

    yield task_status


@pytest.fixture
def test_data_table(flowdb_connection_engine):
    """
    Creates and cleans up a table to simulate the remote table.
    """
    create_sql = """CREATE TABLE IF NOT EXISTS events.sample (
        event_time          timestamptz not null,
        msisdn              varchar(64),
        cell_id             varchar(20)
    );"""
    with flowdb_connection_engine.begin():
        flowdb_connection_engine.execute(create_sql)
    yield flowdb_connection_engine
    with flowdb_connection_engine.begin():
        flowdb_connection_engine.execute("DROP TABLE events.sample CASCADE;")


@pytest.fixture
def populated_test_data_table(test_data_table):
    """
    Populates the simulated remote table with one record for each day.
    """
    populate_sql = f"""
    INSERT INTO events.sample
        VALUES 
            ('2016-06-15 00:01:00'::timestamptz, '{"A"*64}', '{"B"*20}'), 
            ('2016-06-16 00:01:00'::timestamptz, '{"C"*64}', '{"D"*20}'),
            ('2016-06-17 00:01:00'::timestamptz, '{"E"*64}', '{"F"*20}'),
            ('2016-06-18 00:01:00'::timestamptz, '{"G"*64}', '{"H"*20}') 
    """
    with test_data_table.begin():
        test_data_table.execute(populate_sql)
    yield test_data_table


def grow(db_connection, exit_trigger: Event):
    while not exit_trigger.is_set():
        populate_sql = f"""
            INSERT INTO events.sample
                VALUES 
                    ('2016-06-15 00:01:00'::timestamptz, '{"A"*64}', '{"B"*20}');
            """
        with db_connection.begin():
            db_connection.execute(populate_sql)
        sleep(1)


@pytest.fixture
def growing_test_data_table(test_data_table):
    """
    Grows the first day of test data in a background thread, adding a new row every second until terminated.
    """
    exit_trigger = Event()
    grow_thread = Thread(target=grow, args=(test_data_table, exit_trigger))
    grow_thread.start()
    yield test_data_table
    exit_trigger.set()
    grow_thread.join()


@pytest.fixture
def all_tasks():
    yield [
        "create_staging_view",
        "wait_for_data",
        "check_not_in_flux",
        "extract",
        "add_indexes",
        "add_constraints",
        "analyze",
        "attach",
        "update_records",
    ]
