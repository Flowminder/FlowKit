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

if os.environ.get("TESTING", "") == "true":
    logger.info("running in testing environment")
    dag = construct_etl_sensor_dag(
        callable=TEST_ETL_SENSOR_TASK_CALLABLE, default_args=default_args
    )
else:
    logger.info("running in production environment")
    dag = construct_etl_sensor_dag(
        callable=PRODUCTION_ETL_SENSOR_TASK_CALLABLE, default_args=default_args
    )
