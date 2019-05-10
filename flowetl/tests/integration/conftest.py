# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Conftest for flowetl integration tests
"""
import os
import shutil

from time import sleep
from subprocess import DEVNULL, Popen
from pendulum import now, Interval
from airflow.models import DagRun

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
def flowetl_container(docker_client, flowetl_tag):
    """
    Fixture that starts a running flowetl container and
    yields the container object.
    """
    container = docker_client.containers.run(
        f"flowminder/flowetl:{flowetl_tag}", detach=True
    )
    # brief sleep to wait for backing DB to be ready
    sleep(2)
    yield container
    container.kill()
    container.remove()


@pytest.fixture(scope="module")
def airflow_local_setup():
    """
    Init the airflow sqlitedb and start the scheduler with minimal env.
    Clean up aftwards by removing the AIRFLOW_HOME and stopping the
    scheduler. It is necessary to set the AIRFLOW_HOME env variable on
    test invocation otherwise it gets created somewhere else...
    """
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

    yield

    scheduler.terminate()

    shutil.rmtree(env["AIRFLOW_HOME"])
    os.unlink("./scheduler.log")


@pytest.fixture(scope="function")
def airflow_local_pipeline_run():
    """
    Similar to airflow_local_setup_mdl_scope but for starting the scheduler
    with some extra env determined in the test. Also triggers the etl_sensor
    dag causing a subsequent trigger of the etl dag.
    """
    scheduler_to_clean_up = None

    def run_func(extra_env):
        nonlocal scheduler_to_clean_up
        default_env = {
            "AIRFLOW__CORE__DAGS_FOLDER": "./dags",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        }
        env = {**os.environ, **default_env, **extra_env}
        initdb = Popen(
            ["airflow", "initdb"], shell=False, stdout=DEVNULL, stderr=DEVNULL, env=env
        )
        initdb.wait()

        with open("scheduler.log", "w") as f:
            scheduler = Popen(
                ["airflow", "scheduler"], shell=False, stdout=f, stderr=f, env=env
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
            "airflow unpause etl".split(),
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

    shutil.rmtree(os.environ["AIRFLOW_HOME"])
    os.unlink("./scheduler.log")


@pytest.fixture(scope="function")
def wait_for_completion():
    """
    Return a function that waits for the etl dag to be in a specific
    end state. If dag does not reach this state within (arbitarily but
    seems OK...) three minutes raises a TimeoutError.
    """

    def wait_func(end_state):
        time_out = Interval(minutes=3)
        t0 = now()
        while not DagRun.find("etl", state=end_state):
            sleep(1)
            t1 = now()
            if (t1 - t0) > time_out:
                raise TimeoutError
        return end_state

    return wait_func
