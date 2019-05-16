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

from etl.dummy_task_callables import dummy__callable, dummy_failing__callable
from etl.production_task_callables import (
    move_file_and_record_ingestion_state__callable,
    render_and_run_sql__callable,
    success_branch__callable,
)

# callables to be used when testing the structure of the ETL DAG
TEST_TASK_CALLABLES = {
    "init": dummy__callable,
    "extract": dummy__callable,
    "transform": dummy__callable,
    "load": dummy__callable,
    "success_branch": success_branch__callable,
    "archive": dummy__callable,
    "quarantine": dummy__callable,
    "clean": dummy__callable,
    "fail": dummy_failing__callable,
}

# All these are dummy values for now!
mount_paths = {
    "dump": "/dump",
    "ingest": "/ingest",
    "archive": "/archive",
    "quarantine": "/quarantine",
}

db_hook = PostgresHook(postgres_conn_id="flowdb")
config_path = Path("/config")

# callables to be used in production
PRODUCTION_TASK_CALLABLES = {
    "init": partial(
        move_file_and_record_ingestion_state__callable,
        mount_paths=mount_paths,
        from_dir="dump",
        to_dir="ingest",
    ),
    "extract": partial(
        render_and_run_sql__callable,
        db_hook=db_hook,
        config_path=config_path,
        template_name="extract",
    ),
    "transform": partial(
        render_and_run_sql__callable,
        db_hook=db_hook,
        config_path=config_path,
        template_name="transform",
    ),
    "load": partial(
        render_and_run_sql__callable,
        db_hook=db_hook,
        config_path=config_path,
        template_name="load",
    ),
    "success_branch": success_branch__callable,
    "archive": partial(
        move_file_and_record_ingestion_state__callable,
        mount_paths=mount_paths,
        from_dir="ingest",
        to_dir="archive",
    ),
    "quarantine": partial(
        move_file_and_record_ingestion_state__callable,
        mount_paths=mount_paths,
        from_dir="ingest",
        to_dir="quarantine",
    ),
    "clean": partial(
        render_and_run_sql__callable,
        db_hook=db_hook,
        config_path=config_path,
        template_name="clean",
    ),
    "fail": dummy_failing__callable,
}
