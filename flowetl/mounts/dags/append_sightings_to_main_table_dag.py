import os

from airflow import DAG

from datetime import datetime

from flowetl.util import create_staging_dag, create_sighting_dag


dag_staging = create_staging_dag(
    start_date=datetime.strptime(os.getenv("FLOWETL_CSV_START_DATE"), "%Y-%m-%d")
)

dag_sighting = create_sighting_dag(
    start_date=datetime.strptime(os.getenv("FLOWETL_CSV_START_DATE"), "%Y-%m-%d")
)
