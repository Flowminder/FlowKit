# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains utility functions for use in the ETL dag and it's callables
"""
import yaml

from uuid import UUID
from typing import List
from enum import Enum
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator


def construct_etl_dag(
    *, task_callable_mapping: dict, default_args: dict, cdr_type: str
) -> DAG:
    """
    This function returns an Airflow DAG object of the structure
    required for ETL. By passing a dictionary mapping task ID to
    a python callable we can change the functionality of the
    individual tasks whilst maintaining the desired structure
    of the DAG.

    Parameters
    ----------
    task_callable_mapping : dict
         Must contain the following keys;
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
    default_args : dict
        A set of default args to pass to all callables.
        Must containt at least "owner" key and "start" key (which must be a
        pendulum date object)
    cdr_type : str
        The type of CDR that this ETL DAG will process.

    Returns
    -------
    DAG
        Specification of Airflow DAG for ETL

    Raises
    ------
    TypeError
        Exception raised if required keys in task_callable_mapping are not
        present.
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

    with DAG(
        dag_id=f"etl_{cdr_type}", schedule_interval=None, default_args=default_args
    ) as dag:

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


def get_session():
    """
    Dummy for now will get us a session to flowdb
    """
    return "session"


class CDRType(str, Enum):
    CALLS = "calls"
    SMS = "sms"
    MDS = "mds"
    TOPUPS = "topups"


class State(str, Enum):
    INGEST = "ingest"
    ARCHIVE = "archive"
    QUARANTINE = "quarantine"


def find_files(*, dump_path: Path, ignore_filenames=["README.md"]) -> List[Path]:
    """
    Returns a list of Path objects for all files
    found in the dump location.

    Parameters
    ----------
    dump_path : Path
        The location of the dump path

    ignore_filenames : Path
        List of filenames to ignore

    Returns
    -------
    List[Path]
        List of files found
    """
    files = filter(lambda file: file.name not in ignore_filenames, dump_path.glob("*"))
    return files

    #


def generate_temp_table_names(*, cdr_type: CDRType, uuid: UUID) -> dict:
    """
    [summary]

    Parameters
    ----------
    cdr_type : CDRType
        [description]
    uuid : UUID
        [description]

    Returns
    -------
    dict
        [description]
    """
    uuid_sans_underscore = str(uuid).replace("-", "")

    extract_table = f"etl.x{uuid_sans_underscore}"
    transform_table = f"etl.t{uuid_sans_underscore}"
    load_table = f"events.{cdr_type}"

    return {
        "extract_table": extract_table,
        "transform_table": transform_table,
        "load_table": load_table,
    }
