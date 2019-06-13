# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from pendulum import parse
from etl.etl_utils import construct_etl_sensor_dag
from etl.dag_task_callable_mappings import (
    TEST_ETL_SENSOR_TASK_CALLABLE,
    PRODUCTION_ETL_SENSOR_TASK_CALLABLE,
)


def test_construct_etl_sensor_dag_with_test_callable():
    """
    Make sure we get the python callables we expect when using
    construct_etl_sensor_dag with testing callables.
    """
    default_args = {"owner": "bob", "start_date": parse("1900-01-01")}
    task_callable = TEST_ETL_SENSOR_TASK_CALLABLE

    dag = construct_etl_sensor_dag(callable=task_callable, default_args=default_args)

    assert dag.dag_id == f"etl_sensor"

    assert len(dag.tasks) == 1

    assert dag.tasks[0].python_callable is task_callable


def test_construct_etl_sensor_dag_with_production_callable():
    """
    Make sure we get the python callables we expect when using
    construct_etl_sensor_dag with production callables.
    """
    default_args = {"owner": "bob", "start_date": parse("1900-01-01")}
    task_callable = PRODUCTION_ETL_SENSOR_TASK_CALLABLE

    dag = construct_etl_sensor_dag(callable=task_callable, default_args=default_args)

    assert dag.dag_id == f"etl_sensor"

    assert len(dag.tasks) == 1

    assert dag.tasks[0].python_callable is task_callable
