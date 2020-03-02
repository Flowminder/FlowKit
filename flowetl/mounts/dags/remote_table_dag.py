# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Test dags for use in integration tests.
"""

from datetime import datetime, timedelta

from airflow import DAG
from flowetl.util import create_dag

dag = create_dag(
    dag_id="remote_table_dag",
    schedule_interval=None,
    retries=0,
    retry_delay=timedelta(days=1),
    start_date=datetime(2016, 6, 15),
    end_date=datetime(2016, 6, 17),
    cdr_type="calls",
    source_table="events.sample",
    staging_view_sql="create_staging_view.sql",
    data_present_poke_interval=5,
    flux_check_poke_interval=5,
    flux_check_wait_interval=5,
    extract_sql="extract.sql",
    indexes=["msisdn", "location_id", "datetime"],
)
dag.is_paused_upon_creation = False
