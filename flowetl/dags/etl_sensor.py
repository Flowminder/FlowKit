# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
import os
import structlog

# Need to import the DAG class (even if it is not directly
# used in this file) so that Airflow looks here for a DAG.
from airflow import DAG  # pylint: disable=unused-import

from etl.etl_utils import construct_etl_sensor_dag
from etl.dag_task_callable_mappings import (
    TEST_ETL_SENSOR_TASK_CALLABLE,
    PRODUCTION_ETL_SENSOR_TASK_CALLABLE,
)

logger = structlog.get_logger("flowetl")

ETL_SENSOR_TASK_CALLABLES = {
    "testing": TEST_ETL_SENSOR_TASK_CALLABLE,
    "production": PRODUCTION_ETL_SENSOR_TASK_CALLABLE,
}

flowetl_runtime_config = os.environ.get("FLOWETL_RUNTIME_CONFIG", "production")

try:
    etl_sensor_task_callable = ETL_SENSOR_TASK_CALLABLES[flowetl_runtime_config]
except KeyError:
    raise ValueError(
        f"Invalid config name: '{flowetl_runtime_config}'. "
        f"Valid config names are: {list(ETL_SENSOR_TASK_CALLABLES.keys())}"
    )

logger.info(f"Running in {flowetl_runtime_config} environment")
dag = construct_etl_sensor_dag(callable=etl_sensor_task_callable)
