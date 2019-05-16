# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains the definition of callables to be used in the production ETL dag.
"""
import logging
import shutil

from pathlib import Path

from airflow.models import DagRun, BaseOperator
from airflow.hooks.dbapi_hook import DbApiHook

from etl.model import ETLRecord
from etl.etl_utils import get_session

# pylint: disable=unused-argument
def render_and_run_sql__callable(
    *,
    dag_run: DagRun,
    task: BaseOperator,
    db_hook: DbApiHook,
    config_path: Path,
    template_name: str,
    **kwargs,
):
    """
    This function takes information from the DagRun conf to locate
    the correct sql template file, uses the DagRun conf to populate the
    template and runs it against the DB.

    Parameters
    ----------
    dag_run : DagRun
        Passed as part of the Dag context - contains the config.
    task : BaseOperator
        Passed as part of the Dag context - provides access to the instantiated
        operator this callable is running in.
    db_hook : DbApiHook
        A hook to a DB - will most likely be the PostgresHook but could
        be other types of Airflow DB hooks.
    config_path : Path
        Location of flowelt config directory - where templates are stored.
    template_name : str
        The file name sans .sql that we wish to template. Most likely the
        same as the task_id.
    """
    # dag_run.conf["template_path"] -> where the sql templates
    # for this dag run live. Determined by the type of the CDR
    # this dag is ingesting. If this is voice then template_path
    # will be 'etl/voice'.
    template_path = config_path / dag_run.conf["template_path"]

    # template name matches the task_id this is being used
    # in. If this is the transform task then it will be 'transform'
    # and thus the template we use will be 'etl/voice/transform.sql'
    template_path = template_path / f"{template_name}.sql"
    template = open(template_path).read()

    # make use of the operator's templating functionality
    sql = task.render_template("", template, dag_run.conf)

    # run the templated sql against DB
    db_hook.run(sql=sql)


# pylint: disable=unused-argument
def move_file_and_record_ingestion_state__callable(
    *, dag_run: DagRun, mount_paths: dict, from_dir: str, to_dir: str, **kwargs
):
    """
    Function to deal with moving files between various mount directories along
    with recording the state of the ingestion. Since the directory structure
    reflects the state of ingestion we make use of the to_dir location to specify
    the next state of the file being ingested. The actual change to the DB to record
    new state is accomplished in the record_etl_state function.

    Parameters
    ----------
    dag_run : DagRun
        Passed as part of the Dag context - contains the config.
    mount_paths : dict
        A dictionary that stores the locations of each of the various
        mount directories
    from_dir : str
        The location from which the file should be moved
    to_dir : str
        The location to which the file is being moved and the resulting state
        of the file
    """
    from_path = mount_paths[from_dir]
    to_path = mount_paths[to_dir]

    file_name = dag_run.conf["file_name"]
    cdr_type = dag_run.conf["cdr_type"]
    cdr_date = dag_run.conf["cdr_date"]

    file_to_move = from_path / file_name
    shutil.copy(str(file_to_move), str(to_path))
    file_to_move.unlink()

    to_state = to_path.name

    session = get_session()
    ETLRecord.set_state(
        cdr_type=cdr_type, cdr_date=cdr_date, state=to_state, session=session
    )


# pylint: disable=unused-argument
def success_branch__callable(*, dag_run: DagRun, **kwargs):
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
