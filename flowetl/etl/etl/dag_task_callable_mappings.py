# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Mapping task id to a python callable. Allows for the specification of a set of
dummy callables to be used for testing.
"""
from functools import partial
from pathlib import Path

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

from etl.config_parser import get_config_from_file
from etl.dummy_task_callables import (
    dummy__callable,
    dummy_failing__callable,
    dummy_trigger__callable,
)
from etl.production_task_callables import (
    record_ingestion_state__callable,
    run_postload_queries__callable,
    success_branch__callable,
    production_trigger__callable,
)
from etl.postetl_queries import POSTETL_QUERIES_FOR_TYPE

db_hook = PostgresHook(postgres_conn_id="flowdb")
config_path = Path("/mounts/config")
files_path = Path("/mounts/files")
try:
    config = get_config_from_file(config_filepath=config_path / "config.yml")
except FileNotFoundError:
    # If we are testing then there will be no config file and we
    # don't actually need any!
    config = {}

# callables to be used when testing the structure of the ETL DAG
TEST_ETL_TASK_CALLABLES = {
    "init": partial(
        PythonOperator, provide_context=True, python_callable=dummy__callable
    ),
    "extract": partial(
        PythonOperator, provide_context=True, python_callable=dummy__callable
    ),
    "transform": partial(
        PythonOperator, provide_context=True, python_callable=dummy__callable
    ),
    "load": partial(
        PythonOperator, provide_context=True, python_callable=dummy__callable
    ),
    "postload": partial(
        PythonOperator, provide_context=True, python_callable=dummy__callable
    ),
    "success_branch": partial(
        BranchPythonOperator,
        provide_context=True,
        python_callable=success_branch__callable,
    ),
    "archive": partial(
        PythonOperator, provide_context=True, python_callable=dummy__callable
    ),
    "quarantine": partial(
        PythonOperator, provide_context=True, python_callable=dummy__callable
    ),
    "clean": partial(
        PythonOperator, provide_context=True, python_callable=dummy__callable
    ),
    "fail": partial(
        PythonOperator, provide_context=True, python_callable=dummy_failing__callable
    ),
}

# callables to be used in production
PRODUCTION_ETL_TASK_CALLABLES = {
    "init": partial(
        PythonOperator,
        provide_context=True,
        python_callable=partial(record_ingestion_state__callable, to_state="ingest"),
    ),
    "extract": partial(PostgresOperator, postgres_conn_id="flowdb"),
    "transform": partial(PostgresOperator, postgres_conn_id="flowdb"),
    "load": partial(PostgresOperator, postgres_conn_id="flowdb"),
    "postload": partial(
        PythonOperator,
        provide_context=True,
        python_callable=partial(
            run_postload_queries__callable, queries=POSTETL_QUERIES_FOR_TYPE
        ),
    ),
    "success_branch": partial(
        BranchPythonOperator,
        provide_context=True,
        python_callable=success_branch__callable,
    ),
    "archive": partial(
        PythonOperator,
        provide_context=True,
        python_callable=partial(record_ingestion_state__callable, to_state="archive"),
    ),
    "quarantine": partial(
        PythonOperator,
        provide_context=True,
        python_callable=partial(
            record_ingestion_state__callable, to_state="quarantine"
        ),
    ),
    "clean": partial(PostgresOperator, postgres_conn_id="flowdb"),
    "fail": partial(
        PythonOperator, provide_context=True, python_callable=dummy_failing__callable
    ),
}

TEST_ETL_SENSOR_TASK_CALLABLE = dummy_trigger__callable
PRODUCTION_ETL_SENSOR_TASK_CALLABLE = partial(
    production_trigger__callable,
    files_path=files_path,
    cdr_type_config=config.get("etl", {}),
)
