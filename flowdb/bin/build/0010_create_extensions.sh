#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.



set -e

#
#  Perform all actions as $POSTGRES_USER
#
export PGUSER="$POSTGRES_USER"
EXTENSIONS=('postgis' 'postgis_raster' 'postgis_topology' 'fuzzystrmatch' \
            'file_fdw' 'uuid-ossp' 'plpython3u' \
            'tsm_system_rows' 'pgrouting' 'pldbgapi' 'pg_median_utils'\
            'ogr_fdw' 'tds_fdw' 'anon_func')

#
#  Create the 'template_postgis' template db
#
psql --dbname="$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE template_postgis;
    UPDATE pg_database SET datistemplate = TRUE 
      WHERE datname = 'template_postgis';
EOSQL

#
#  Load extensions into both databases.
#  Only the PostGIS extensions are required
#  to be loaded into the template_postgis
#  template database. 
#
for EXT in "${EXTENSIONS[@]}"; do
    for DB in template_postgis "$POSTGRES_DB"; do

echo "Loading $EXT extension into $DB"
psql --dbname="$DB" <<EOSQL
    CREATE EXTENSION IF NOT EXISTS "$EXT";
EOSQL

    done
done

#
#  Create servers in both available
#  databases.
#
for DB in template_postgis "$POSTGRES_DB"; do

echo "Creating extension servers in $DB."
psql --dbname="$DB" <<-EOSQL
    CREATE SERVER csv_fdw 
        FOREIGN DATA WRAPPER file_fdw;
EOSQL

done
