import logging
import os

from airflow import DAG
from pendulum import parse

from etl.etl_utils import construct_etl_sensor_dag
from etl.dag_task_callable_mappings import (
    TEST_ETL_SENSOR_TASK_CALLABLE,
    PRODUCTION_ETL_SENSOR_TASK_CALLABLE,
)

default_args = {"owner": "flowminder", "start_date": parse("1900-01-01")}

if os.environ.get("TESTING", "") == "true":
    logging.info("running in testing environment")
    dag = construct_etl_sensor_dag(
        callable=TEST_ETL_SENSOR_TASK_CALLABLE, default_args=default_args
    )
else:
    logging.info("running in production environment")
    dag = construct_etl_sensor_dag(
        callable=PRODUCTION_ETL_SENSOR_TASK_CALLABLE, default_args=default_args
    )
