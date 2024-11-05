# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Test dags for use in integration tests.
"""

from airflow import DAG
from datetime import datetime
from flowetl.util import get_qa_checks

with DAG(
    "test_qa_dag",
    schedule_interval="@daily",
    start_date=datetime(2016, 6, 15),
    end_date=datetime(2016, 6, 17),
    params=dict(cdr_type="calls"),
) as dag:
    get_qa_checks()
