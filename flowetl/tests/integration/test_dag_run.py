# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Test dag run logic
"""


def test_quarantine_branch(airflow_local_pipeline_run, wait_for_completion):
    """
    Tests that correct tasks run, with correct end state, when ETL is
    not successful. We fail each of the tasks init, extract, transform and load.
    """
    end_state = "failed"
    fail_state = "success"
    dag_type = "testing"

    airflow_local_pipeline_run, expected_task_states = airflow_local_pipeline_run
    airflow_local_pipeline_run()
    final_etl_state = wait_for_completion(
        end_state=end_state, fail_state=fail_state, dag_id=f"etl_{dag_type}"
    )
    assert final_etl_state == end_state
    from airflow.models import DagRun

    etl_dag = DagRun.find(f"etl_{dag_type}", state=end_state)[0]

    task_states = {task.task_id: task.state for task in etl_dag.get_task_instances()}
    assert task_states == expected_task_states
