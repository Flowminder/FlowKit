# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Skeleton specification for ETL DAG
"""
import os
import structlog

from pathlib import Path

# need to import and not use so that airflow looks here for a DAG
from airflow import DAG  # pylint: disable=unused-import

from pendulum import parse
from etl.dag_task_callable_mappings import (
    TEST_ETL_TASK_CALLABLES,
    PRODUCTION_ETL_TASK_CALLABLES,
)
from etl.etl_utils import construct_etl_dag, CDRType
from etl.config_parser import validate_config, get_config_from_file

logger = structlog.get_logger("flowetl")
default_args = {"owner": "flowminder", "start_date": parse("1900-01-01")}

# Determine if we are in a testing environment - use dummy callables if so
if os.environ.get("TESTING", "") == "true":
    task_callable_mapping = TEST_ETL_TASK_CALLABLES
    logger.info("running in testing environment")

    dag = construct_etl_dag(
        **task_callable_mapping, default_args=default_args, cdr_type="testing"
    )
else:
    task_callable_mapping = PRODUCTION_ETL_TASK_CALLABLES
    logger.info("running in production environment")

    # read and validate the config file before creating the DAGs
    global_config_dict = get_config_from_file(
        config_filepath=Path("/mounts/config/config.yml")
    )
    validate_config(global_config_dict=global_config_dict)

    default_args = global_config_dict["default_args"]

    # create DAG for each cdr_type
    for cdr_type in CDRType:
        # Ensure `cdr_type` is a string (e.g. "sms", instead of the raw value `CDRType.SMS`)
        # so that interpolation in SQL templates works as expected.
        cdr_type = cdr_type.value

        globals()[f"etl_{cdr_type}"] = construct_etl_dag(
            **task_callable_mapping, default_args=default_args, cdr_type=cdr_type
        )
