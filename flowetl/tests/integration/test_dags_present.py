# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Make sure that airflow is able to pick up the correct DAGs
"""
from pathlib import Path

import pytest


# pylint: disable=unused-argument


def test_dags_present(airflow_local_setup):
    """
    Test that the correct dags are parsed
    """
    from airflow.models import DagBag

    assert set(
        DagBag(
            dag_folder=str(Path(__file__).parent.parent.parent / "dags"),
            include_examples=False,
        ).dag_ids
    ) == set(["etl_testing", "etl_sensor"])


@pytest.mark.parametrize(
    "dag_name,expected_task_list",
    [
        (
            "etl_testing",
            [
                "init",
                "extract",
                "transform",
                "success_branch",
                "load",
                "postload",
                "archive",
                "quarantine",
                "clean",
                "fail",
            ],
        ),
        ("etl_sensor", ["sense"]),
    ],
)
def test_correct_tasks(airflow_local_setup, dag_name, expected_task_list):
    """
    Test that each dag has the tasks expected
    """
    from airflow.models import DagBag

    dag = DagBag(
        dag_folder=str(Path(__file__).parent.parent.parent / "dags"),
        include_examples=False,
    ).dags[dag_name]
    assert set(dag.task_ids) == set(expected_task_list)
