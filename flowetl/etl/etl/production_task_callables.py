# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains the definition of callables to be used in the production ETL dag.
"""
import pendulum
import re
import structlog

from pathlib import Path
from uuid import uuid1

from airflow.models import DagRun
from airflow.api.common.experimental.trigger_dag import trigger_dag

from etl.model import ETLRecord, ETLPostQueryOutcome
from etl.etl_utils import (
    CDRType,
    get_session,
    find_files_matching_pattern,
    extract_date_from_filename,
    find_distinct_dates_in_table,
)

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
def run_postload_queries__callable(*, queries: dict, dag_run: DagRun, **kwargs):
    """
    Function to deal with running and recording the outcome of informative
    post-load ETL queries for the cdr_type of the running DAG.

    Parameters
    ----------
    queries : dict
        The dictionary that maps CDRType to the array of queries to run.
    dag_run : DagRun
        Passed as part of the Dag context - contains the config.
    """
    cdr_type = dag_run.conf["cdr_type"]
    cdr_date = dag_run.conf["cdr_date"]

    session = get_session()

    if cdr_type not in queries:
        raise ValueError(
            f"Attempted to run queries for non-existing CDRType {cdr_type}"
        )

    for query in queries[cdr_type]:
        query_result = query(cdr_date=cdr_date, session=session)
        optional_comment_or_description = (
            query_result["optional_comment_or_description"]
            if "optional_comment_or_description" in query_result
            else ""
        )

        ETLPostQueryOutcome.set_outcome(
            cdr_type=cdr_type,
            cdr_date=cdr_date,
            type_of_query_or_check=query_result["type_of_query_or_check"],
            outcome=query_result["outcome"],
            optional_comment_or_description=optional_comment_or_description,
            session=session,
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
    Function that determines which files in files/ or which external database
    tables should be processed and triggers the correct ETL dag with config based
    on the CDR date derived from the filename or data present.

    Parameters
    ----------
    dag_run : DagRun
        Passed as part of the Dag context - contains the config.
    files_path : Path
        Location of files directory
    cdr_type_config : dict
        ETL config for each cdr type
    """
    session = get_session()

    for cdr_type, cfg in cdr_type_config.items():
        cdr_type = CDRType(cdr_type)

        source_type = cfg["source"]["source_type"]
        logger.info(f"Config for {cdr_type!r} ({source_type}): {cfg}")

        if source_type == "csv":
            filename_pattern = cfg["source"]["filename_pattern"]
            logger.info(f"Filename pattern: {filename_pattern!r}")
            all_files_found = find_files_matching_pattern(
                files_path=files_path, filename_pattern=filename_pattern
            )
            dates_found = {
                filename: extract_date_from_filename(filename, filename_pattern)
                for filename in all_files_found
            }
            unprocessed_files_and_dates = {
                filename: date
                for filename, date in dates_found.items()
                if ETLRecord.can_process(
                    cdr_type=cdr_type, cdr_date=date, session=session
                )
            }
            for file, cdr_date in unprocessed_files_and_dates.items():
                uuid = uuid1()
                cdr_date_str = cdr_date.strftime("%Y%m%d")
                execution_date = pendulum.Pendulum(
                    cdr_date.year, cdr_date.month, cdr_date.day
                )
                full_file_path = str(files_path.joinpath(file).absolute())
                config = {
                    "cdr_type": cdr_type,
                    "cdr_date": cdr_date,
                    "file_name": file,
                    "full_file_path": full_file_path,
                    "template_path": f"etl/{cdr_type}",
                }
                trigger_dag(
                    f"etl_{cdr_type}",
                    execution_date=execution_date,
                    run_id=f"{cdr_type.upper()}_{cdr_date_str}-{str(uuid)}",
                    conf=config,
                    replace_microseconds=False,
                )
        elif source_type == "sql":
            source_table = cfg["source"]["table_name"]

            # Extract unprocessed dates from source_table

            # TODO: this requires a full parse of the existing data so may not be
            # the most be efficient if a lot of data is present (esp. data that has
            # already been processed). If it turns out too sluggish might be good to
            # think about a more efficient way to determine dates with unprocessed data.
            dates_present = find_distinct_dates_in_table(
                session, source_table, event_time_col="event_time"
            )
            unprocessed_dates = [
                date
                for date in dates_present
                if ETLRecord.can_process(
                    cdr_type=cdr_type, cdr_date=date, session=session
                )
            ]
            logger.info(f"Dates found: {dates_present}")
            logger.info(f"Unprocessed dates: {unprocessed_dates}")

            for cdr_date in unprocessed_dates:
                uuid = uuid1()
                cdr_date_str = cdr_date.strftime("%Y%m%d")
                execution_date = pendulum.Pendulum(
                    cdr_date.year, cdr_date.month, cdr_date.day
                )
                config = {
                    "cdr_type": cdr_type,
                    "cdr_date": cdr_date,
                    "source_table": source_table,
                }
                trigger_dag(
                    f"etl_{cdr_type}",
                    execution_date=execution_date,
                    run_id=f"{cdr_type.upper()}_{cdr_date_str}-{str(uuid)}",
                    conf=config,
                    replace_microseconds=False,
                )
        else:
            raise ValueError(f"Invalid source type: '{source_type}'")
