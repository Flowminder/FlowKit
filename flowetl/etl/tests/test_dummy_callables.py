# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests that the testing dummy callables behave as expected.
"""
import pytest
from unittest.mock import patch, Mock
from uuid import uuid1
from pendulum import utcnow
from airflow.exceptions import DagNotFound

from etl.dummy_task_callables import (
    dummy__callable,
    dummy_failing__callable,
    dummy_trigger__callable,
)


def test_dummy__callable_succeeds_with_no_TASK_TO_FAIL_env_var_set(
    create_fake_task_instance,
):
    """
    Test that when no TASK_TO_FAIL env var set the dummy callable
    succeeds.
    """
    dag_run = {}
    task_id = "init"
    task_instance = create_fake_task_instance(task_id=task_id)

    dummy__callable(dag_run=dag_run, task_instance=task_instance)


def test_dummy__callable_succeeds_when_TASK_TO_FAIL_env_var_is_not_same_as_task_id(
    create_fake_task_instance,
):
    """
    Test that when TASK_TO_FAIL env var is different from the task id the dummy
    callable succeeds.
    """
    dag_run = {}
    TASK_TO_FAIL = "init"
    task_id = "extract"
    task_instance = create_fake_task_instance(task_id=task_id)

    with patch("os.environ", {"TASK_TO_FAIL": TASK_TO_FAIL}):
        dummy__callable(dag_run=dag_run, task_instance=task_instance)


def test_dummy__callable_fails_when_TASK_TO_FAIL_env_var_is_same_as_task_id(
    create_fake_task_instance,
):
    """
    Test that if TASK_TO_FAIL is same as task id the dummy_callable raises
    an exception.
    """
    dag_run = {}
    TASK_TO_FAIL = "init"
    task_id = TASK_TO_FAIL
    task_instance = create_fake_task_instance(task_id=task_id)

    with patch("os.environ", {"TASK_TO_FAIL": TASK_TO_FAIL}):
        with pytest.raises(Exception):
            dummy__callable(dag_run=dag_run, task_instance=task_instance)


def test_dummy_failing__callable():
    """
    Test that the dummy_failing_callable raises an Exception
    """
    with pytest.raises(Exception):
        dummy_failing__callable(dag_run={})


def test_dummy_trigger__callable(monkeypatch):
    """
    Test that the dummy trigger callable works as expected.
    """
    mock_trigger_dag = Mock()
    monkeypatch.setattr("etl.dummy_task_callables.trigger_dag", mock_trigger_dag)

    uuid = uuid1()
    monkeypatch.setattr("etl.dummy_task_callables.uuid1", lambda: uuid)

    now = utcnow()
    monkeypatch.setattr("etl.dummy_task_callables.utcnow", lambda: now)

    try:
        dummy_trigger__callable(dag_run={})
    except DagNotFound:
        # the testing etl dag doesn't exist!
        pass

    mock_trigger_dag.assert_called_once_with(
        "etl_testing", run_id=str(uuid), execution_date=now
    )
