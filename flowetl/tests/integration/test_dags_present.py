# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
TODO
"""

import pytest

from airflow.models import DagBag

# pylint: disable=unused-argument
def test_dags_present(airflow_local_setup_mdl_scope):
    """
    Test that the correct dags are parsed
    """
    assert set(DagBag(dag_folder="./dags", include_examples=False).dag_ids) == set(
        ["etl", "etl_sensor"]
    )


@pytest.mark.parametrize(
    "dag_name,expected_task_list",
    [
        (
            "etl",
            [
                "init",
                "extract",
                "transform",
                "success_branch",
                "load",
                "archive",
                "quarantine",
                "clean",
                "fail",
            ],
        ),
        ("etl_sensor", ["sense"]),
    ],
)
def test_correct_tasks(airflow_local_setup_mdl_scope, dag_name, expected_task_list):
    """
    Test that each dag has the tasks expected
    """
    dag = DagBag(dag_folder="./dags", include_examples=False).dags[dag_name]
    assert set(dag.task_ids) == set(expected_task_list)
