#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

set -euo pipefail

EXTRA_DATABASES="flowdb_revised_schema"

echo "-----------------------------------------------------"
echo " * Creating additional databases: ${EXTRA_DATABASES} "
echo "-----------------------------------------------------"

for DB in $EXTRA_DATABASES; do

    psql --dbname="$POSTGRES_DB" -c "
        CREATE DATABASE $DB
        WITH OWNER flowdb;
    "

    psql --dbname="$DB" -c "CREATE EXTENSION IF NOT EXISTS postgis";

done
