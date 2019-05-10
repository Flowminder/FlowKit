# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Skeleton specification for ETL sensor DAG
"""
import logging
import uuid

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag

from pendulum import now, parse

default_args = {"owner": "flowminder", "start_date": parse("1900-01-01")}

# pylint: disable=unused-argument
def dummy_trigger_callable(*, dag_run: DagRun, **kwargs):
    """
    Dummy callable that triggers ETL dag
    """
    logging.info(dag_run)
    trigger_dag("etl", run_id=str(uuid.uuid1()), execution_date=now())


with DAG(dag_id="etl_sensor", schedule_interval=None, default_args=default_args) as dag:

    sense = PythonOperator(
        task_id="sense", python_callable=dummy_trigger_callable, provide_context=True
    )
