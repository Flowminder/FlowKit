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
    dag_id="calls",
    schedule_interval="@daily",
    retries=0,
    retry_delay=timedelta(days=1),
    start_date=datetime(2019, 12, 21),
    end_date=datetime(2019, 12, 27),
    cdr_type="calls",
    data_present_poke_interval=5,
    flux_check_poke_interval=5,
    flux_check_wait_interval=5,
    extract_sql="extract_calls.sql",
    indexes=["msisdn_counterpart", "location_id", "datetime", "tac"],
    cluster_field="msisdn",
    source_table="sample_data_fdw",
    staging_view_sql="stage_calls.sql",
    null="Undefined",
)
