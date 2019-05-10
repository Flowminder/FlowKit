# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Skeleton specification for ETL DAG
"""
import logging
import os

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from pendulum import parse

default_args = {"owner": "flowminder", "start_date": parse("1900-01-01")}

# pylint: disable=unused-argument
def dummy_callable(*, dag_run: DagRun, task_instance: TaskInstance, **kwargs):
    """
    Dummy python callable - raises an exception if the environment variable TASK_FAIL is defined, otherwise succeeds silently.
    """
    logging.info(dag_run)
    if os.environ["TASK_FAIL"] == task_instance.task_id:
        raise Exception


def dummy_failing_callable(*, dag_run: DagRun, **kwargs):
    """
    Dummy python callable raising an exception
    """
    logging.info(dag_run)
    raise Exception


def success_branch_callable(*, dag_run: DagRun, **kwargs):
    """
    Dummy branch callable
    """
    previous_task_failures = [
        dag_run.get_task_instance(task_id).state == "failed"
        for task_id in ["init", "extract", "transform", "load"]
    ]

    logging.info(dag_run)

    if any(previous_task_failures):
        branch = "quarantine"
    else:
        branch = "archive"

    return branch


with DAG(dag_id="etl", schedule_interval=None, default_args=default_args) as dag:

    init = PythonOperator(
        task_id="init", python_callable=dummy_callable, provide_context=True
    )
    extract = PythonOperator(
        task_id="extract", python_callable=dummy_callable, provide_context=True
    )
    transform = PythonOperator(
        task_id="transform", python_callable=dummy_callable, provide_context=True
    )
    success_branch = BranchPythonOperator(
        task_id="success_branch",
        python_callable=success_branch_callable,
        provide_context=True,
        trigger_rule="all_done",
    )
    load = PythonOperator(
        task_id="load", python_callable=dummy_callable, provide_context=True
    )
    archive = PythonOperator(
        task_id="archive", python_callable=dummy_callable, provide_context=True
    )
    quarantine = PythonOperator(
        task_id="quarantine", python_callable=dummy_callable, provide_context=True
    )
    clean = PythonOperator(
        task_id="clean",
        python_callable=dummy_callable,
        provide_context=True,
        trigger_rule="all_done",
    )
    fail = PythonOperator(
        task_id="fail", python_callable=dummy_failing_callable, provide_context=True
    )

    # Define upstream/downstream relationships between airflow tasks
    init >> extract >> transform >> load >> success_branch  # pylint: disable=pointless-statement
    success_branch >> archive >> clean  # pylint: disable=pointless-statement
    quarantine >> clean  # pylint: disable=pointless-statement
    success_branch >> quarantine >> fail  # pylint: disable=pointless-statement
