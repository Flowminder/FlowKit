# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Test dag run logic
"""

import pytest

from airflow.models import DagRun


def test_archive_branch(airflow_local_pipeline_run, wait_for_completion):
    """
    Tests that correct tasks run when ETL is succesful
    """
    end_state = "success"

    # passing empty TASK_FAIL to signal no task should fail
    airflow_local_pipeline_run({"TASK_FAIL": ""})
    final_etl_state = wait_for_completion(end_state)
    assert final_etl_state == end_state

    etl_dag = DagRun.find("etl", state=end_state)[0]

    task_states = {task.task_id: task.state for task in etl_dag.get_task_instances()}
    assert task_states == {
        "init": "success",
        "extract": "success",
        "transform": "success",
        "success_branch": "success",
        "load": "success",
        "archive": "success",
        "quarantine": "skipped",
        "clean": "success",
        "fail": "skipped",
    }


@pytest.mark.parametrize(
    "task_to_fail,expected_task_states",
    [
        (
            "init",
            {
                "init": "failed",
                "extract": "upstream_failed",
                "transform": "upstream_failed",
                "success_branch": "success",
                "load": "upstream_failed",
                "archive": "skipped",
                "quarantine": "success",
                "clean": "success",
                "fail": "failed",
            },
        ),
        (
            "extract",
            {
                "init": "success",
                "extract": "failed",
                "transform": "upstream_failed",
                "success_branch": "success",
                "load": "upstream_failed",
                "archive": "skipped",
                "quarantine": "success",
                "clean": "success",
                "fail": "failed",
            },
        ),
        (
            "transform",
            {
                "init": "success",
                "extract": "success",
                "transform": "failed",
                "success_branch": "success",
                "load": "upstream_failed",
                "archive": "skipped",
                "quarantine": "success",
                "clean": "success",
                "fail": "failed",
            },
        ),
        (
            "load",
            {
                "init": "success",
                "extract": "success",
                "transform": "success",
                "success_branch": "success",
                "load": "failed",
                "archive": "skipped",
                "quarantine": "success",
                "clean": "success",
                "fail": "failed",
            },
        ),
    ],
)
def test_quarantine_branch(
    airflow_local_pipeline_run, wait_for_completion, task_to_fail, expected_task_states
):
    """
    Tests that correct tasks run, with correct end state, when ETL is
    not successful. We fail each of the tasks init, extract, transform and load.
    """
    end_state = "failed"
    airflow_local_pipeline_run({"TASK_FAIL": task_to_fail})
    final_etl_state = wait_for_completion(end_state)
    assert final_etl_state == end_state

    etl_dag = DagRun.find("etl", state=end_state)[0]

    task_states = {task.task_id: task.state for task in etl_dag.get_task_instances()}
    assert task_states == expected_task_states
