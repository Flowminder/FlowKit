# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for the construction of the ETL DAG
"""
import pytest
from pendulum import parse
from unittest.mock import Mock

from airflow.operators.python_operator import PythonOperator

from etl.etl_utils import construct_etl_dag
from etl.dag_task_callable_mappings import (
    TEST_ETL_TASK_CALLABLES,
    PRODUCTION_ETL_TASK_CALLABLES,
)


def test_construct_etl_dag_with_test_callables():
    """
    Make sure that the DAG returned has the correct task callables as
    specified in the task_callable_mapping argument. Use TEST_ETL_TASK_CALLABLES
    mapping.
    """
    task_callable_mapping = {
        t: Mock(wraps=v) for (t, v) in TEST_ETL_TASK_CALLABLES.items()
    }
    cdr_type = "spaghetti"

    dag = construct_etl_dag(**task_callable_mapping, cdr_type=cdr_type)

    assert dag.dag_id == f"etl_{cdr_type}"

    dag_task_callable_mapping = {
        t.task_id: t.python_callable for t in dag.tasks if isinstance(t, PythonOperator)
    }
    expected_dag_task_callable_mapping = {
        t.task_id: TEST_ETL_TASK_CALLABLES[t.task_id](task_id=t.task_id).python_callable
        for t in dag.tasks
        if isinstance(t, PythonOperator)
    }
    assert dag_task_callable_mapping == expected_dag_task_callable_mapping
    [t.assert_called_once() for _, t in task_callable_mapping.items()]


def test_construct_etl_dag_with_production_callables():
    """
    Make sure that the DAG returned has the correct task callables as
    specified in the task_callable_mapping argument. Use PRODUCTION_ETL_TASK_CALLABLES
    mapping.
    """
    task_callable_mapping = {
        t: Mock(wraps=v) for (t, v) in PRODUCTION_ETL_TASK_CALLABLES.items()
    }
    cdr_type = "spaghetti"

    dag = construct_etl_dag(**task_callable_mapping, cdr_type=cdr_type)

    assert dag.dag_id == f"etl_{cdr_type}"

    dag_task_callable_mapping = {
        t.task_id: t.python_callable for t in dag.tasks if isinstance(t, PythonOperator)
    }
    expected_dag_task_callable_mapping = {
        t.task_id: PRODUCTION_ETL_TASK_CALLABLES[t.task_id](
            task_id=t.task_id
        ).python_callable
        for t in dag.tasks
        if isinstance(t, PythonOperator)
    }
    assert dag_task_callable_mapping == expected_dag_task_callable_mapping
    [t.assert_called_once() for _, t in task_callable_mapping.items()]


def test_construct_etl_dag_sets_owner_to_airflow():
    """
    Make sure that the DAG owner of the constructed DAG is Flowminder.
    """
    task_callable_mapping = TEST_ETL_TASK_CALLABLES
    cdr_type = "spaghetti"

    dag = construct_etl_dag(**task_callable_mapping, cdr_type=cdr_type)

    assert dag.owner == "flowminder"


def test_construct_etl_dag_sets_start_date_correctly():
    """
    Make sure that the start_date of the DAG is the expected date in the past.
    """
    task_callable_mapping = TEST_ETL_TASK_CALLABLES
    cdr_type = "spaghetti"

    dag = construct_etl_dag(**task_callable_mapping, cdr_type=cdr_type)

    assert dag.start_date == parse("1900-01-01")


def test_construct_etl_dag_fails_with_incorrect_mapping_keys():
    """
    If the dictionary we pass to task_callable_mapping does not have
    correct keys we get a TypeError.
    """
    task_callable_mapping = {}
    cdr_type = "spaghetti"

    # pylint: disable=unused-variable
    with pytest.raises(TypeError):
        dag = construct_etl_dag(**task_callable_mapping, cdr_type=cdr_type)
