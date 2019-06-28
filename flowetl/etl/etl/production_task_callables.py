# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains the definition of callables to be used in the production ETL dag.
"""
import structlog

from pathlib import Path
from uuid import uuid1

from airflow.models import DagRun
from airflow.api.common.experimental.trigger_dag import trigger_dag

from etl.model import ETLRecord
from etl.etl_utils import get_session, find_files, filter_files, get_config

logger = structlog.get_logger("flowetl")


# pylint: disable=unused-argument
def record_ingestion_state__callable(*, dag_run: DagRun, to_state: str, **kwargs):
    """
    Function to deal with recording the state of the ingestion. The actual
    change to the DB to record new state is accomplished in the
    ETLRecord.set_state function.

    Parameters
    ----------
    dag_run : DagRun
        Passed as part of the Dag context - contains the config.
    to_state : str
        The the resulting state of the file
    """
    cdr_type = dag_run.conf["cdr_type"]
    cdr_date = dag_run.conf["cdr_date"]

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

    logger.info(f"Dag run: {dag_run}")

    if any(previous_task_failures):
        branch = "quarantine"
    else:
        branch = "archive"

    return branch


def production_trigger__callable(
    *, dag_run: DagRun, files_path: Path, cdr_type_config: dict, **kwargs
):
    """
    Function that determines which files in files/ should be processed
    and triggers the correct ETL dag with config based on filename.

    Parameters
    ----------
    dag_run : DagRun
        Passed as part of the Dag context - contains the config.
    files_path : Path
        Location of files directory
    cdr_type_config : dict
        ETL config for each cdr type
    """

    found_files = find_files(files_path=files_path)
    logger.info(found_files)
    logger.info(f"Files found: {found_files}")

    # remove files that either do not match a pattern
    # or have been processed successfully already...
    filtered_files = filter_files(
        found_files=found_files, cdr_type_config=cdr_type_config
    )
    logger.info(
        f"Files found that match the filename pattern and have not been processed: {filtered_files}"
    )

    # what to do with these!?
    bad_files = list(set(found_files) - set(filtered_files))
    logger.info(f"Bad files found: {bad_files}")

    for file in filtered_files:
        config = get_config(file_name=file.name, cdr_type_config=cdr_type_config)

        cdr_type = config["cdr_type"]
        cdr_date = config["cdr_date"]
        uuid = uuid1()
        trigger_dag(
            f"etl_{cdr_type}",
            execution_date=cdr_date,
            run_id=f"{file.name}-{str(uuid)}",
            conf=config,
            replace_microseconds=False,
        )
