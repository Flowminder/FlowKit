# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains the definition of callables to be used in the production ETL dag.
"""
import logging

from pathlib import Path

from airflow.models import DagRun, BaseOperator
from airflow.hooks.dbapi_hook import DbApiHook


def render_sql_callable(
    *,
    dag_run: DagRun,
    task: BaseOperator,
    db_hook: DbApiHook,
    config_path: Path,
    template_name: str,
    **kwargs,
):
    template_path = config_path / dag_run.conf["template_path"]

    template_path = template_path / f"{template_name}.sql"
    template = open(template_path).read()

    sql = task.render_template("", template, dag_run.conf)

    db_hook.run(sql=sql)


# pylint: disable=unused-argument
def success_branch_callable(*, dag_run: DagRun, **kwargs):
    """
    Function to determine if we should follow the quarantine or
    the archive branch. If no downstream tasks have failed we follow
    archive branch and quarantine otherwise.
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
