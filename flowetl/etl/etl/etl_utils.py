# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains utility functions for use in the ETL dag and it's callables
"""
import os
import pendulum
import re

from typing import List, Callable
from enum import Enum
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def construct_etl_sensor_dag(*, callable: Callable, default_args: dict) -> DAG:
    """
    This function constructs the sensor single task DAG that triggers ETL
    DAGS with correct config based on filename.

    Parameters
    ----------
    callable : Callable
        The sense callable that deals with finding files and triggering
        ETL DAGs
    default_args : dict
        Default arguments for DAG

    Returns
    -------
    DAG
        Airflow DAG
    """
    with DAG(
        dag_id=f"etl_sensor", schedule_interval=None, default_args=default_args
    ) as dag:
        sense = PythonOperator(
            task_id="sense", python_callable=callable, provide_context=True
        )

    return dag


def construct_etl_dag(
    *,
    init: Callable,
    extract: Callable,
    transform: Callable,
    load: Callable,
    success_branch: Callable,
    archive: Callable,
    quarantine: Callable,
    clean: Callable,
    fail: Callable,
    default_args: dict,
    cdr_type: str,
    config_path: str = "/mounts/config",
) -> DAG:
    """
    This function returns an Airflow DAG object of the structure
    required for ETL. The individual tasks are passed as python
    Callables.

    Parameters
    ----------
    init : Callable
        The init task callable.
    extract : Callable
        The extract task callable.
    transform : Callable
        The transform task callable.
    load : Callable
        The load task callable.
    success_branch : Callable
        The success_branch task callable.
    archive : Callable
        The archive task callable.
    quarantine : Callable
        The quarantine task callable.
    clean : Callable
        The clean task callable.
    fail : Callable
        The fail task callable.
    default_args : dict
        A set of default args to pass to all callables.
        Must containt at least "owner" key and "start" key (which must be a
        pendulum date object)
    cdr_type : str
        The type of CDR that this ETL DAG will process.
    config_path : str
        The config path used to look for the sql templates.

    Returns
    -------
    DAG
        Specification of Airflow DAG for ETL
    """

    with DAG(
        dag_id=f"etl_{cdr_type}",
        schedule_interval=None,
        default_args=default_args,
        template_searchpath=config_path,  # template paths will be relative to this
        user_defined_macros={
            "get_extract_view": lambda execution_date: f"etl.x{ cdr_type }_{ execution_date }",
            "get_transform_table": lambda execution_date: f"etl.t{ cdr_type }_{ execution_date }",
            "get_load_table": lambda execution_date: f"events.{ cdr_type }_{ execution_date }",
            "cdr_type": cdr_type,
        },
    ) as dag:

        init = init(task_id="init")
        extract = extract(task_id="extract", sql=f"etl/{cdr_type}/extract.sql")
        transform = transform(task_id="transform", sql=f"etl/{cdr_type}/transform.sql")
        load = load(task_id="load", sql="fixed_sql/load.sql")
        success_branch = success_branch(
            task_id="success_branch", trigger_rule="all_done"
        )
        archive = archive(task_id="archive")
        quarantine = quarantine(task_id="quarantine")
        clean = clean(
            task_id="clean", sql="fixed_sql/clean.sql", trigger_rule="all_done"
        )
        fail = fail(task_id="fail")

        # Define upstream/downstream relationships between airflow tasks
        init >> extract >> transform >> load >> success_branch  # pylint: disable=pointless-statement
        success_branch >> archive >> clean  # pylint: disable=pointless-statement
        quarantine >> clean  # pylint: disable=pointless-statement
        success_branch >> quarantine >> fail  # pylint: disable=pointless-statement

    return dag


def get_session(*, postgres_conn_id="flowdb"):
    """
    Constructs a sqlalchmy session for use with
    the ETLRecord model in flowdb. Can construct
    connection to other DB's specified by
    postgres_conn_id and reflected in the ENV by
    variables of the form AIRFLOW_CONN_${postgres_conn_id}

    Parameters
    ----------
    postgres_conn_id : str, optional
        The ID of a connection known to airflow.
        Should exist as an ENV var of the form
        AIRFLOW_CONN_${postgres_conn_id}.
        default "flowdb"
    """
    conn_env_var = f"AIRFLOW_CONN_{postgres_conn_id.upper()}"
    if conn_env_var not in os.environ:
        raise ValueError(f"{conn_env_var} not set")

    engine = create_engine(
        "postgresql://", creator=PostgresHook(postgres_conn_id).get_conn
    )
    return sessionmaker(bind=engine)()


class CDRType(str, Enum):
    """
    CDR type enum
    """

    CALLS = "calls"
    SMS = "sms"
    MDS = "mds"
    TOPUPS = "topups"


class State(str, Enum):
    """
    ETL state enum
    """

    INGEST = "ingest"
    ARCHIVE = "archive"
    QUARANTINE = "quarantine"


def find_files(*, files_path: Path, ignore_filenames=["README.md"]) -> List[Path]:
    """
    Returns a list of Path objects for all files
    found in the files location.

    Parameters
    ----------
    files_path : Path
        The location of the files path

    ignore_filenames : Path
        List of filenames to ignore

    Returns
    -------
    List[Path]
        List of files found
    """
    files = filter(lambda file: file.name not in ignore_filenames, files_path.glob("*"))
    return list(files)


def find_files_matching_pattern(
    *, files_path: Path, filename_pattern: str
) -> List[str]:
    """
    Returns a list of Path objects for all files found in the files location that match the given pattern.

    Parameters
    ----------
    files_path : Path
        The location of the files path
    filename_pattern : str
        Regular expression to match the filenames against

    Returns
    -------
    List[str]
        List of matching files found
    """
    all_files = sorted([file for file in files_path.glob("*")])
    matching_files = [
        file.name for file in all_files if re.fullmatch(filename_pattern, file.name)
    ]
    return sorted(matching_files)


def extract_date_from_filename(filename: str, filename_pattern: str) -> pendulum.Date:
    """
    Return date extracted from the given filename based on the pattern.

    Note: this assumes that `filename_pattern` contains exactly one group
    (marked by the `(` and `)` metacharacters) which represents the date.

    Parameters
    ----------
    filename : str
        Filename which includes the date, for example `CALLS_20160101.csv`.
    filename_pattern : str
        The pattern used to match the filename and extract the date, e.g. `CALLS_(\d{8}).csv.gz`

    Returns
    -------
    pendulum.Date
        Date extracted from the filename.
    """
    m = re.fullmatch(filename_pattern, str(filename))
    if m is None:
        raise ValueError(
            f"Filename '{filename}' does not match the pattern '{filename_pattern}'"
        )
    date_str = m.group(1)
    return pendulum.parse(date_str).date()
