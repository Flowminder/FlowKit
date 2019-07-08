# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Contains utility functions for use in the ETL dag and it's callables
"""
import re
import os

from uuid import UUID
from typing import List, Callable
from enum import Enum
from pathlib import Path
from pendulum import parse
from pendulum.date import Date as pendulumDate

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from etl import model


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
    postload: Callable,
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
    postload : Callable
        The postload task callable.
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
            "get_extract_table": lambda execution_date: f"etl.x{ cdr_type }_{ execution_date }",
            "get_transform_table": lambda execution_date: f"etl.t{ cdr_type }_{ execution_date }",
            "get_load_table": lambda execution_date: f"events.{ cdr_type }_{ execution_date }",
            "cdr_type": cdr_type,
        },
    ) as dag:

        init = init(task_id="init")
        extract = extract(task_id="extract", sql=f"etl/{cdr_type}/extract.sql")
        transform = transform(task_id="transform", sql=f"etl/{cdr_type}/transform.sql")
        load = load(task_id="load", sql="fixed_sql/load.sql")
        postload = postload(task_id="postload")
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
        init >> extract >> transform >> load >> postload >> success_branch  # pylint: disable=pointless-statement
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


def filter_files(*, found_files: List, cdr_type_config: dict):
    """
    Takes a list of files and filters them based on two
    factors;
    1. Does the filename match any CDR type pattern if not remove
    2. Has the file been successfully ingested if so remove

    Parameters
    ----------
    found_files : List
        List of found files should be Path objects
    cdr_type_config : dict
        config dict containing patterns for
        each cdr type


    Returns
    -------
    List
        Files that can be processed by ETL DAG
    """
    filtered_files = []
    for file in found_files:
        try:
            # try to parse file name
            parsed_file_name_config = parse_file_name(
                file_name=file.name, cdr_type_config=cdr_type_config
            )
        except ValueError:
            # couldnt parse moving on
            continue

        cdr_type = parsed_file_name_config["cdr_type"]
        cdr_date = parsed_file_name_config["cdr_date"]

        session = get_session()
        if model.ETLRecord.can_process(
            cdr_type=cdr_type, cdr_date=cdr_date, session=session
        ):
            filtered_files.append(file)

    return filtered_files


def get_config(*, file_name: str, cdr_type_config: dict) -> dict:
    """
    Create DAG config that is based on filename

    Parameters
    ----------
    file_name : str
        name of file to construct config for
    cdr_type_config : dict
        config dict containing patterns for
        each cdr type

    Returns
    -------
    dict
        Dictionary with config for this filename
    """
    parsed_file_name_config = parse_file_name(
        file_name=file_name, cdr_type_config=cdr_type_config
    )
    template_path = f"etl/{parsed_file_name_config['cdr_type']}"
    other_config = {"file_name": file_name, "template_path": template_path}
    return {**parsed_file_name_config, **other_config}


def parse_file_name(*, file_name: str, cdr_type_config: dict) -> dict:
    """
    Function to parse date of data and cdr type from filename.
    Makes use of patterns specified in global config.

    Parameters
    ----------
    file_name : str
        The file name to parse
    cdr_type_config : dict
        The config for each CDR type which contains
        patterns to match against.

    Returns
    -------
    dict
        contains files cdr type and the date associated
        to the data
    """
    file_cdr_type, file_cdr_date = None, None
    for cdr_type in CDRType:
        pattern = cdr_type_config[cdr_type]["pattern"]
        m = re.fullmatch(pattern, file_name)
        if m:
            file_cdr_type = cdr_type
            file_cdr_date = parse(m.groups()[0])

    if file_cdr_type and file_cdr_date:
        parsed_file_info = {"cdr_type": file_cdr_type, "cdr_date": file_cdr_date}
    else:
        raise ValueError("No pattern match found")

    return parsed_file_info
