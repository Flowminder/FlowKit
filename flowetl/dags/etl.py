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

# Need to import the DAG class (even if it is not directly
# used in this file) so that Airflow looks here for a DAG.
from airflow import DAG  # pylint: disable=unused-import

from etl.dag_task_callable_mappings import (
    TEST_ETL_TASK_CALLABLES,
    PRODUCTION_ETL_TASK_CALLABLES,
)
from etl.etl_utils import construct_etl_dag, CDRType
from etl.config_parser import (
    get_config_from_file,
    validate_config,
    fill_config_default_values,
)

logger = structlog.get_logger("flowetl")

ETL_TASK_CALLABLES = {
    "testing": TEST_ETL_TASK_CALLABLES,
    "production": PRODUCTION_ETL_TASK_CALLABLES,
}

flowetl_runtime_config = os.environ.get("FLOWETL_RUNTIME_CONFIG", "production")

# Determine if we are in a testing environment - use dummy callables if so
if flowetl_runtime_config == "testing":
    task_callable_mapping = TEST_ETL_TASK_CALLABLES
    logger.info("Running in testing environment")

    dag = construct_etl_dag(**task_callable_mapping, cdr_type="testing")
elif flowetl_runtime_config == "production":
    task_callable_mapping = PRODUCTION_ETL_TASK_CALLABLES
    logger.info("Running in production environment")

    # read and validate the config file before creating the DAGs
    global_config_dict = get_config_from_file(
        config_filepath=Path("/mounts/config/config.yml")
    )
    validate_config(global_config_dict)
    global_config_dict = fill_config_default_values(global_config_dict)

    # create DAG for each cdr_type
    for cdr_type in CDRType:
        # Ensure `cdr_type` is a string (e.g. "sms", instead of the raw value `CDRType.SMS`)
        # so that interpolation in SQL templates works as expected.
        cdr_type = cdr_type.value

        globals()[f"etl_{cdr_type}"] = construct_etl_dag(
            **task_callable_mapping, cdr_type=cdr_type
        )
else:
    raise ValueError(
        f"Invalid config name: '{flowetl_runtime_config}'. "
        f"Valid config names are: {list(ETL_TASK_CALLABLES.keys())}"
    )
