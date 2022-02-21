#!/usr/bin/env bash

set -euo pipefail

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

airflow db init
airflow users create -r Admin -u "${FLOWETL_AIRFLOW_ADMIN_USERNAME:?Need to set FLOWETL_AIRFLOW_ADMIN_USERNAME non-empty}" -e ${FLOWETL_AIRFLOW_ADMIN_EMAIL:-admin@example.com} -f ${FLOWETL_AIRFLOW_ADMIN_FIRSTNAME:-admin} -l ${FLOWETL_AIRFLOW_ADMIN_LASTNAME:-user} -p "${FLOWETL_AIRFLOW_ADMIN_PASSWORD:?Need to set FLOWETL_AIRFLOW_ADMIN_PASSWORD non-empty}"
airflow pools -s postgres_etl ${FLOWETL_AIRFLOW_PG_POOL_SLOT_COUNT:-4} "Allows an upper bound on number of concurrent high load postgres dags."
