#!/bin/bash
set -e
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


#
#  ROLES
#  ------
#
#  Here we define the main database roles. Three roles
#  are defined:
#
#   * flowdb:    the database administrator. It can
#               modify all tables in the database 
#               and change properties from other users. 
#               This user is mainly designed for ingesting 
#               data into the events table.
#
#   * flowmachine:  for users that need access to raw data.
#               They can read all data available in the database and 
#               can write to most tables. Can't write to the tables under 
#               the events schema.
#
#   * $FLOWAPI_FLOWDB_USER: user that reads data from the public table
#               and the geography table (for reference 
#               data). This user is designed mainly 
#               for visualization applications.
#

export PGUSER="$POSTGRES_USER"

if [ ! -e /run/secrets/POSTGRES_PASSWORD -a -z "$POSTGRES_PASSWORD" ];
then
    echo "No password supplied for superuser!"
    echo "Set the POSTGRES_PASSWORD environment variable, or provide the POSTGRES_PASSWORD secret"
    exit 1
fi

if [ -e /run/secrets/FLOWMACHINE_FLOWDB_PASSWORD ];
then
    echo "Using secrets for flowmachine user password."
    FLOWMACHINE_FLOWDB_PASSWORD=$(< /run/secrets/FLOWMACHINE_FLOWDB_PASSWORD)
fi

if [ -e /run/secrets/FLOWAPI_FLOWDB_USER ];
then
    echo "Using secrets for flowapi user."
    FLOWAPI_FLOWDB_USER=$(< /run/secrets/FLOWAPI_FLOWDB_USER)
fi

if [ -e /run/secrets/FLOWAPI_FLOWDB_PASSWORD ];
then
    echo "Using secrets for flowapi user password."
    FLOWAPI_FLOWDB_PASSWORD=$(< /run/secrets/FLOWAPI_FLOWDB_PASSWORD)
fi

psql --dbname="$POSTGRES_DB" -c "REVOKE CONNECT ON DATABASE $POSTGRES_DB FROM PUBLIC;"

if [[ $FLOWMACHINE_FLOWDB_PASSWORD ]]
    then
        psql --dbname="$POSTGRES_DB" -c "CREATE ROLE flowmachine WITH LOGIN PASSWORD '$FLOWMACHINE_FLOWDB_PASSWORD';"
    else
        echo "No password supplied for 'flowmachine' user: $FLOWMACHINE_FLOWDB_PASSWORD"
        exit 1
fi

if [[ $FLOWAPI_FLOWDB_PASSWORD ]]
    then
        psql --dbname="$POSTGRES_DB" -c "CREATE ROLE $FLOWAPI_FLOWDB_USER WITH LOGIN PASSWORD '$FLOWAPI_FLOWDB_PASSWORD';"
    else
        echo "No password supplied for '$FLOWAPI_FLOWDB_USER' user: $FLOWAPI_FLOWDB_PASSWORD"
        exit 1
fi

#
#  Roles can connect and create tables and
#  schemas.
#
psql --dbname="$POSTGRES_DB" -c "GRANT CONNECT ON DATABASE $POSTGRES_DB TO flowmachine;"
psql --dbname="$POSTGRES_DB" -c "GRANT CONNECT ON DATABASE $POSTGRES_DB TO $FLOWAPI_FLOWDB_USER;"
psql --dbname="$POSTGRES_DB" -c "GRANT CREATE ON DATABASE $POSTGRES_DB TO flowmachine;"
psql --dbname="$POSTGRES_DB" -c "GRANT CREATE ON DATABASE $POSTGRES_DB TO $FLOWAPI_FLOWDB_USER;"

#
#  Adding permissions.
#
declare -a schema_list_permissive=("cache" "geography" "population" "elevation")
for schema in "${schema_list_permissive[@]}"
do
    echo "Granting permissions to flowmachine on $schema."
    psql --dbname="$POSTGRES_DB" -c "
        BEGIN;
            ALTER DEFAULT PRIVILEGES
                IN SCHEMA $schema
                GRANT ALL ON TABLES TO flowmachine;

            ALTER DEFAULT PRIVILEGES FOR ROLE flowmachine
                IN SCHEMA $schema
                GRANT ALL ON TABLES TO flowmachine;

            GRANT ALL PRIVILEGES 
                ON ALL TABLES 
                IN SCHEMA $schema TO flowmachine;

            GRANT USAGE
                ON SCHEMA $schema
                TO flowmachine;

            GRANT ALL  
                ON ALL TABLES 
                IN SCHEMA $schema TO flowmachine;
            
            GRANT CREATE
                ON SCHEMA $schema
                TO flowmachine;
        COMMIT;
        "
done

declare -a schema_list_restricted=("events" "dfs" "infrastructure" "routing" "interactions" "etl")
for schema in "${schema_list_restricted[@]}"
do
    echo "Restricting permissions to flowmachine on $schema."
    psql --dbname="$POSTGRES_DB" -c "
        BEGIN;
            GRANT USAGE
                ON SCHEMA $schema
                TO flowmachine;

            GRANT SELECT
                ON ALL TABLES 
                IN SCHEMA $schema TO flowmachine;

            ALTER DEFAULT PRIVILEGES 
                IN SCHEMA $schema
                GRANT SELECT ON SEQUENCES TO flowmachine;

            ALTER DEFAULT PRIVILEGES 
                IN SCHEMA $schema
                GRANT SELECT ON TABLES TO flowmachine;
        END;
        "
done

echo "Give flowmachine role read and update access to cache_touches sequence"
psql --dbname="$POSTGRES_DB" -c "
    BEGIN;
        GRANT USAGE, SELECT, UPDATE ON SEQUENCE cache.cache_touches TO flowmachine;
    COMMIT;
    "

echo "Give $FLOWAPI_FLOWDB_USER role read access to tables created under cache schema."
psql --dbname="$POSTGRES_DB" -c "
    BEGIN;
        GRANT USAGE
            ON SCHEMA cache
            TO $FLOWAPI_FLOWDB_USER;
        ALTER DEFAULT PRIVILEGES
            IN SCHEMA cache
            GRANT SELECT ON TABLES TO $FLOWAPI_FLOWDB_USER;
        ALTER DEFAULT PRIVILEGES FOR ROLE flowmachine
            IN SCHEMA cache
            GRANT SELECT ON TABLES TO $FLOWAPI_FLOWDB_USER;
    END;
    "

psql --dbname="$POSTGRES_DB" -c "
        ALTER DEFAULT PRIVILEGES 
            IN SCHEMA public
            GRANT SELECT ON TABLES TO PUBLIC
        "

# Allow $FLOWAPI_FLOWDB_USER role to read geography tables

psql --dbname="$POSTGRES_DB" -c "
        ALTER DEFAULT PRIVILEGES
                IN SCHEMA geography
                GRANT SELECT ON TABLES TO $FLOWAPI_FLOWDB_USER;
        GRANT USAGE ON SCHEMA geography TO $FLOWAPI_FLOWDB_USER;
        "