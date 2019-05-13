import pytest

from unittest.mock import patch, MagicMock, PropertyMock
from etl.dummy_task_callables import dummy_callable, dummy_failing_callable


def test_dummy_callable_succeeds_with_no_TASK_TO_FAIL_env_var_set():
    """
    Test that when no TASK_TO_FAIL env var set the dummy callable
    succeeds.
    """
    dag_run = {}
    task_instance = MagicMock()
    task_id_mock = PropertyMock(return_value="init")
    type(task_instance).task_id = task_id_mock

    dummy_callable(dag_run=dag_run, task_instance=task_instance)


def test_dummy_callable_succeeds_when_TASK_TO_FAIL_env_var_is_not_same_as_task_id():
    """
    Test that when TASK_TO_FAIL env var is different from the task id the dummy
    callable succeeds.
    """
    TASK_TO_FAIL = "init"
    task_id = "extract"
    dag_run = {}
    task_instance = MagicMock()
    task_id_mock = PropertyMock(return_value=task_id)
    type(task_instance).task_id = task_id_mock

    with patch("os.environ", {"TASK_TO_FAIL": TASK_TO_FAIL}):
        dummy_callable(dag_run=dag_run, task_instance=task_instance)


def test_dummy_callable_fails_when_TASK_TO_FAIL_env_var_is_same_as_task_id():
    """
    Test that if TASK_TO_FAIL is same as task id the dummy_callable raises
    an exception.
    """
    TASK_TO_FAIL = "init"
    dag_run = {}
    task_instance = MagicMock()
    task_id_mock = PropertyMock(return_value=TASK_TO_FAIL)
    type(task_instance).task_id = task_id_mock

    with patch("os.environ", {"TASK_TO_FAIL": TASK_TO_FAIL}):
        with pytest.raises(Exception):
            dummy_callable(dag_run=dag_run, task_instance=task_instance)


def test_dummy_failing_callable():
    """
    Test that the dummy_failing_callable raises an Exception
    """
    with pytest.raises(Exception):
        dummy_failing_callable(dag_run={})
