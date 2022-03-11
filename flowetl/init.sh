#!/usr/bin/env bash

set -euo pipefail

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

echo "Running db init"
airflow db init
echo "Creating admin user"
airflow users create -r Admin -u "${FLOWETL_AIRFLOW_ADMIN_USERNAME:?Need to set FLOWETL_AIRFLOW_ADMIN_USERNAME non-empty}" -e ${FLOWETL_AIRFLOW_ADMIN_EMAIL:-admin@example.com} -f ${FLOWETL_AIRFLOW_ADMIN_FIRSTNAME:-admin} -l ${FLOWETL_AIRFLOW_ADMIN_LASTNAME:-user} -p "${FLOWETL_AIRFLOW_ADMIN_PASSWORD:?Need to set FLOWETL_AIRFLOW_ADMIN_PASSWORD non-empty}"
echo "Creating pool"
airflow pools set postgres_etl ${FLOWETL_AIRFLOW_PG_POOL_SLOT_COUNT:-4} "Allows an upper bound on number of concurrent high load postgres dags."
if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
    # With the "Local" and "Sequential" executors it should all run in one container.
    echo "Starting scheduler"
    exec "airflow" "scheduler"
fi
