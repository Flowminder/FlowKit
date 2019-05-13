import pytest
from pendulum import parse
from pendulum.parsing.exceptions import ParserError

from airflow.exceptions import AirflowException

from etl.etl_utils import construct_etl_dag
from etl.dag_task_callable_mappings import (
    TEST_TASK_CALLABLES,
    PRODUCTION_TASK_CALLABLES,
)


def test_construct_etl_dag_with_test_callables():
    """
    Make sure that the DAG returned has the correct task callables as
    specified in the task_callable_mapping argument. Use TEST_TASK_CALLABLES
    mapping.
    """
    default_args = {"owner": "bob", "start_date": parse("1900-01-01")}
    task_callable_mapping = TEST_TASK_CALLABLES

    dag = construct_etl_dag(
        task_callable_mapping=task_callable_mapping, default_args=default_args
    )

    dag_task_callable_mapping = {t.task_id: t.python_callable for t in dag.tasks}
    assert dag_task_callable_mapping == TEST_TASK_CALLABLES


def test_construct_etl_dag_with_production_callables():
    """
    Make sure that the DAG returned has the correct task callables as
    specified in the task_callable_mapping argument. Use PRODUCTION_TASK_CALLABLES
    mapping.
    """
    default_args = {"owner": "bob", "start_date": parse("1900-01-01")}
    task_callable_mapping = PRODUCTION_TASK_CALLABLES

    dag = construct_etl_dag(
        task_callable_mapping=task_callable_mapping, default_args=default_args
    )

    dag_task_callable_mapping = {t.task_id: t.python_callable for t in dag.tasks}
    assert dag_task_callable_mapping == PRODUCTION_TASK_CALLABLES


def test_construct_etl_dag_fails_with_no_start_date():
    """
    Make sure we get an exception if default_args does not contain a start_date
    """
    default_args = {"owner": "bob"}
    task_callable_mapping = TEST_TASK_CALLABLES

    with pytest.raises(AirflowException):
        dag = construct_etl_dag(
            task_callable_mapping=task_callable_mapping, default_args=default_args
        )


def test_construct_etl_dag_with_no_owner_defaults_to_airflow():
    """
    Make sure that if we pass no owner in default_args the owner is
    Airflow.
    """
    default_args = {"start_date": parse("1900-01-01")}
    task_callable_mapping = TEST_TASK_CALLABLES

    dag = construct_etl_dag(
        task_callable_mapping=task_callable_mapping, default_args=default_args
    )

    assert dag.owner == "Airflow"


def test_construct_etl_dag_fails_with_bad_start_date():
    """
    If the start_date is not a valid date we get an error
    """
    default_args = {"owner": "bob", "start_date": "bob_time"}
    task_callable_mapping = TEST_TASK_CALLABLES

    with pytest.raises(ParserError):
        dag = construct_etl_dag(
            task_callable_mapping=task_callable_mapping, default_args=default_args
        )


def test_construct_etl_dag_fails_with_incorrect_mapping_keys():
    """
    If the dictionary we pass to task_callable_mapping does not have
    correct keys we get a TypeError.
    """
    default_args = {"owner": "bob", "start_date": "bob_time"}
    task_callable_mapping = {}

    with pytest.raises(TypeError):
        dag = construct_etl_dag(
            task_callable_mapping=task_callable_mapping, default_args=default_args
        )
