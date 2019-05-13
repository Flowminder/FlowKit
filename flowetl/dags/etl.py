# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Skeleton specification for ETL DAG
"""
import logging
import os

from airflow import DAG  # pylint: disable=unused-import
from pendulum import parse
from etl.dag_task_callable_mapings import TEST_TASK_CALLABLES, PRODUCTION_TASK_CALLABLES
from etl.etl_utils import construct_etl_dag

default_args = {"owner": "flowminder", "start_date": parse("1900-01-01")}

# Determine if we are in a testing environment - use dummy callables if so
if os.environ.get("TESTING", "") == "true":
    task_callable_mapping = TEST_TASK_CALLABLES
    logging.info("running in testing environment")
else:
    task_callable_mapping = PRODUCTION_TASK_CALLABLES
    logging.info("running in production environment")

dag = construct_etl_dag(
    task_callable_mapping=task_callable_mapping, default_args=default_args
)
