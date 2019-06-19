# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
import os
import structlog

# need to import and not use so that airflow looks here for a DAG
from airflow import DAG  # pylint: disable=unused-import

from pendulum import parse

from etl.etl_utils import construct_etl_sensor_dag
from etl.dag_task_callable_mappings import (
    TEST_ETL_SENSOR_TASK_CALLABLE,
    PRODUCTION_ETL_SENSOR_TASK_CALLABLE,
)

logger = structlog.get_logger("flowetl")

default_args = {"owner": "flowminder", "start_date": parse("1900-01-01")}

try:
    flowetl_runtime_config = os.environ["FLOWETL_RUNTIME_CONFIG"]
except KeyError:
    raise RuntimeError("Must set FLOWETL_RUNTIME_CONFIG env var.")

ETL_SENSOR_TASK_CALLABLES = {
    "testing": TEST_ETL_SENSOR_TASK_CALLABLE,
    "production": PRODUCTION_ETL_SENSOR_TASK_CALLABLE,
}

try:
    etl_sensor_task_callable = ETL_SENSOR_TASK_CALLABLES[flowetl_runtime_config]
except KeyError:
    raise ValueError(
        f"Invalid config name: '{flowetl_runtime_config}'. "
        f"Valid config names are: {list(ETL_SENSOR_TASK_CALLABLES.keys())}"
    )

logger.info(f"Running in {flowetl_runtime_config} environment")
dag = construct_etl_sensor_dag(
    callable=etl_sensor_task_callable, default_args=default_args
)
