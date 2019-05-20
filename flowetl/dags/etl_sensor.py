# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Skeleton specification for ETL sensor DAG
"""
import os
import logging

from uuid import uuid1
from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag

from pendulum import now, parse

from etl.etl_utils import CDRType, generate_temp_table_names

default_args = {"owner": "flowminder", "start_date": parse("1900-01-01")}

# pylint: disable=unused-argument
def dummy_trigger_callable(*, dag_run: DagRun, **kwargs):
    """
    Dummy callable that triggers ETL dag
    """
    logging.info(dag_run)

    if os.environ.get("TESTING", "") == "true":
        # If testing then only one DAG and we kick this off
        logging.info("Triggering ETL in testing environment")
        trigger_dag("etl_testing", run_id=str(uuid.uuid1()), execution_date=now())
    else:
        # otherwise for now (will change!) kick all possible DAGs off
        logging.info("Triggering ETL in production environment")
        temp_base_conf = {
            "file_name": "bob.csv",
            "template_path": "some_path",
            "cdr_type": "spaghetti",
            "cdr_date": "2016-01-01",
        }

        for cdr_type in CDRType:
            uuid = uuid1()

            temp_table_names = generate_temp_table_names(uuid=uuid, cdr_type=cdr_type)
            trigger_dag(
                f"etl_{cdr_type}",
                run_id=str(uuid),
                execution_date=now(),
                conf={**temp_base_conf, **temp_table_names},
            )


with DAG(dag_id="etl_sensor", schedule_interval=None, default_args=default_args) as dag:

    sense = PythonOperator(
        task_id="sense", python_callable=dummy_trigger_callable, provide_context=True
    )
