# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains utility functions for use in the ETL dag and it's callables
"""

from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator


def construct_etl_dag(*, task_callable_mapping: dict, default_args: dict) -> DAG:
    """
    This function returns an Airflow DAG object of the structure
    required for ETL. By passing a dictionary mapping task ID to
    a python callable we can change the functionality of the
    individual tasks whilst maintaining the desired structure
    of the DAG.

    Arguments:
        task_callable_mapping {dict} -- Must contain the
        following keys;
            "init"
            "extract"
            "transform"
            "load"
            "success_branch"
            "archive"
            "quarantine"
            "clean"
            "fail"
        The value for each key should be a python callable with at a minimum
        the following signature foo(*,**kwargs).

        default_args {dict} -- a set of default args to pass to all callables.
        Must containt at least "owner" key and "start" key (which must be a
        pendulum date object)

    Returns:
        DAG -- Specification of Airflow DAG for ETL
    """

    # make sure we have the correct keys
    expected_keys = set(
        [
            "init",
            "extract",
            "transform",
            "load",
            "success_branch",
            "archive",
            "quarantine",
            "clean",
            "fail",
        ]
    )
    if set(task_callable_mapping.keys()) != expected_keys:
        raise TypeError("task_callable_mapping argument does not contain correct keys")

    with DAG(dag_id="etl", schedule_interval=None, default_args=default_args) as dag:

        init = PythonOperator(
            task_id="init",
            python_callable=task_callable_mapping["init"],
            provide_context=True,
        )
        extract = PythonOperator(
            task_id="extract",
            python_callable=task_callable_mapping["extract"],
            provide_context=True,
        )
        transform = PythonOperator(
            task_id="transform",
            python_callable=task_callable_mapping["transform"],
            provide_context=True,
        )
        load = PythonOperator(
            task_id="load",
            python_callable=task_callable_mapping["load"],
            provide_context=True,
        )
        success_branch = BranchPythonOperator(
            task_id="success_branch",
            python_callable=task_callable_mapping["success_branch"],
            provide_context=True,
            trigger_rule="all_done",
        )
        archive = PythonOperator(
            task_id="archive",
            python_callable=task_callable_mapping["archive"],
            provide_context=True,
        )
        quarantine = PythonOperator(
            task_id="quarantine",
            python_callable=task_callable_mapping["quarantine"],
            provide_context=True,
        )
        clean = PythonOperator(
            task_id="clean",
            python_callable=task_callable_mapping["clean"],
            provide_context=True,
            trigger_rule="all_done",
        )
        fail = PythonOperator(
            task_id="fail",
            python_callable=task_callable_mapping["fail"],
            provide_context=True,
        )

        # Define upstream/downstream relationships between airflow tasks
        init >> extract >> transform >> load >> success_branch  # pylint: disable=pointless-statement
        success_branch >> archive >> clean  # pylint: disable=pointless-statement
        quarantine >> clean  # pylint: disable=pointless-statement
        success_branch >> quarantine >> fail  # pylint: disable=pointless-statement

    return dag
