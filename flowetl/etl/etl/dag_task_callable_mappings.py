# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Mapping task id to a python callable. Allows for the specification of a set of
dummy callables to be used for testing.
"""
import os
from functools import partial
from pathlib import Path

from airflow.hooks.postgres_hook import PostgresHook

from etl.config_parser import get_config_from_file
from etl.dummy_task_callables import (
    dummy__callable,
    dummy_failing__callable,
    dummy_trigger__callable,
)
from etl.production_task_callables import (
    move_file_and_record_ingestion_state__callable,
    render_and_run_sql__callable,
    success_branch__callable,
    trigger__callable,
)

mount_paths = {
    "dump": Path("/mounts/dump"),
    "ingest": Path("/mounts/ingest"),
    "archive": Path("/mounts/archive"),
    "quarantine": Path("/mounts/quarantine"),
}

db_hook = PostgresHook(postgres_conn_id="flowdb")
config_path = Path("/mounts/config")
try:
    config = get_config_from_file(config_filepath=config_path / "config.yml")
except FileNotFoundError:
    # If we are testing then there will be no config file and we
    # don't actually need any!
    config = {}

# callables to be used when testing the structure of the ETL DAG
TEST_ETL_TASK_CALLABLES = {
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

# callables to be used in production
PRODUCTION_ETL_TASK_CALLABLES = {
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
        fixed_sql=True,
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
        fixed_sql=True,
    ),
    "fail": dummy_failing__callable,
}

TEST_ETL_SENSOR_TASK_CALLABLE = dummy_trigger__callable
PRODUCTION_ETL_SENSOR_TASK_CALLABLE = partial(
    trigger__callable,
    dump_path=mount_paths["dump"],
    cdr_type_config=config.get("etl", {}),
)
