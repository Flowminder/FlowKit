# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from unittest.mock import Mock

from etl.production_task_callables import success_branch__callable


def test_success_branch__callable_success(create_fake_task_instance):
    """
    Test that we get the archive branch when all downstream tasks
    have been successful
    """
    init_ti = create_fake_task_instance(state="success")
    extract_ti = create_fake_task_instance(state="success")
    transform_ti = create_fake_task_instance(state="success")
    load_ti = create_fake_task_instance(state="success")
    task_dict = {
        "init": init_ti,
        "extract": extract_ti,
        "transform": transform_ti,
        "load": load_ti,
    }

    fake_dag_run = Mock()
    fake_dag_run.get_task_instance.side_effect = lambda task_id: task_dict[task_id]

    branch = success_branch__callable(dag_run=fake_dag_run)
    assert branch == "archive"


def test_success_branch__callable_fail(create_fake_task_instance):
    """
    Test that if one downstream task fails we get the quarantine branch
    """
    init_ti = create_fake_task_instance(state="success")
    extract_ti = create_fake_task_instance(state="success")
    transform_ti = create_fake_task_instance(state="failed")
    load_ti = create_fake_task_instance(state="success")
    task_dict = {
        "init": init_ti,
        "extract": extract_ti,
        "transform": transform_ti,
        "load": load_ti,
    }

    fake_dag_run = Mock()
    fake_dag_run.get_task_instance.side_effect = lambda task_id: task_dict[task_id]

    branch = success_branch__callable(dag_run=fake_dag_run)
    assert branch == "quarantine"
