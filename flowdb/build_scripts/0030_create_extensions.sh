#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

set -euo pipefail

EXTENSIONS=('postgis' 'postgis_topology' 'fuzzystrmatch' \
            'file_fdw' 'uuid-ossp' 'plpythonu' \
            'tsm_system_rows' 'pgrouting' 'pldbgapi')

#
#  Load extensions into the database.
#  Only the PostGIS extensions are required
#  to be loaded into the template_postgis
#  template database.
#
for EXT in "${EXTENSIONS[@]}"; do

echo "Loading $EXT extension into $POSTGRES_DB"
psql --dbname="$POSTGRES_DB" -c <<EOSQL
    CREATE EXTENSION IF NOT EXISTS "$EXT";
EOSQL

done

#
#  Create foreign server for the CSV foreign data wrapper.
#
echo "Creating extension servers in $POSTGRES_DB."
psql --dbname="$POSTGRES_DB" <<-EOSQL
    CREATE SERVER csv_fdw FOREIGN DATA WRAPPER file_fdw;
EOSQL

done
