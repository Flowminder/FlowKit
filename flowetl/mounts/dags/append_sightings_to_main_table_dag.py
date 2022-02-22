import os

from airflow import DAG

from datetime import datetime

from flowetl.util import create_staging_dag


dag = create_staging_dag(
    start_date=datetime.strptime(os.getenv("FLOWETL_CSV_START_DATE"), "%Y-%m-%d"),
    event_types=os.getenv("FLOWETL_EVENT_TYPES", "call,location,sms,mds,topup").split(
        ","
    ),
)
