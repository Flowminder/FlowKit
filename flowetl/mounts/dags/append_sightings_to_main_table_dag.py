import os
from pathlib import Path

from airflow import DAG
from datetime import datetime

from flowetl.util import create_staging_dag

# Hack to make sure the template path follows the import around
from flowetl.operators.staging.sql import __file__ as template_path

template_folder = (
    "/" + os.getenv("SOURCE_TREE") + "/flowetl/flowetl/flowetl/operators/staging/sql"
)

start_string = os.getenv("FLOWETL_CSV_START_DATE")
event_type_string = os.getenv("FLOWETL_EVENT_TYPES")


dag = create_staging_dag(
    start_date=datetime.strptime(start_string, "%Y-%m-%d"),
    event_types=event_type_string.split(","),
)
