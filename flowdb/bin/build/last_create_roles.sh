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
#   * $FLOWMACHINE_FLOWDB_USER:  the for $FLOWMACHINE_FLOWDB_USERs that need access to raw data.
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

if [ ! -e /run/secrets/POSTGRES_PASSWORD_FILE -a -z "$POSTGRES_PASSWORD" ];
then
    echo "No password supplied for superuser!"
    echo "Set the POSTGRES_PASSWORD environment variable, or provide the POSTGRES_PASSWORD_FILE secret"
    exit 1
fi

if [ -e /run/secrets/FLOWMACHINE_DB_USER ];
then
    echo "Using secrets for analyst user."
    FLOWMACHINE_FLOWDB_USER=$(< /run/secrets/FLOWMACHINE_DB_USER)
fi

if [ -e /run/secrets/FLOWMACHINE_FLOWDB_PASS ];
then
    echo "Using secrets for analyst password."
    FLOWMACHINE_FLOWDB_PASS=$(< /run/secrets/FLOWMACHINE_FLOWDB_PASS)
fi

if [ -e /run/secrets/FLOWAPI_FLOWDB_USER ];
then
    echo "Using secrets for reporter user."
    FLOWAPI_FLOWDB_USER=$(< /run/secrets/FLOWAPI_FLOWDB_USER)
fi

if [ -e /run/secrets/FLOWAPI_DB_PASS ];
then
    echo "Using secrets for reporter password."
    FLOWAPI_FLOWDB_PASS=$(< /run/secrets/FLOWAPI_DB_PASS)
fi

psql --dbname="$POSTGRES_DB" -c "REVOKE CONNECT ON DATABASE $POSTGRES_DB FROM PUBLIC;"

if [[ $FLOWMACHINE_FLOWDB_PASS ]]
    then
        psql --dbname="$POSTGRES_DB" -c "CREATE ROLE $FLOWMACHINE_FLOWDB_USER WITH LOGIN PASSWORD '$FLOWMACHINE_FLOWDB_PASS';"
    else
        echo "No password supplied for '$FLOWMACHINE_FLOWDB_USER' user: $FLOWMACHINE_FLOWDB_PASS"
        exit 1
fi

if [[ $FLOWAPI_FLOWDB_PASS ]]
    then
        psql --dbname="$POSTGRES_DB" -c "CREATE ROLE $FLOWAPI_FLOWDB_USER WITH LOGIN PASSWORD '$FLOWAPI_FLOWDB_PASS';"
    else
        echo "No password supplied for '$FLOWAPI_FLOWDB_USER' user: $FLOWAPI_FLOWDB_PASS"
        exit 1
fi

#
#  Roles can connect and create tables and
#  schemas.
#
psql --dbname="$POSTGRES_DB" -c "GRANT CONNECT ON DATABASE $POSTGRES_DB TO $FLOWMACHINE_FLOWDB_USER;"
psql --dbname="$POSTGRES_DB" -c "GRANT CONNECT ON DATABASE $POSTGRES_DB TO $FLOWAPI_FLOWDB_USER;"
psql --dbname="$POSTGRES_DB" -c "GRANT CREATE ON DATABASE $POSTGRES_DB TO $FLOWMACHINE_FLOWDB_USER;"
psql --dbname="$POSTGRES_DB" -c "GRANT CREATE ON DATABASE $POSTGRES_DB TO $FLOWAPI_FLOWDB_USER;"

#
#  Adding permissions.
#
declare -a schema_list_permissive=("cache" "results" "features" "geography" "population" "elevation")
for schema in "${schema_list_permissive[@]}"
do
    echo "Granting permissions to $FLOWMACHINE_FLOWDB_USER on $schema."
    psql --dbname="$POSTGRES_DB" -c "
        BEGIN;
            ALTER DEFAULT PRIVILEGES
                IN SCHEMA $schema
                GRANT ALL ON TABLES TO $FLOWMACHINE_FLOWDB_USER;

            ALTER DEFAULT PRIVILEGES FOR ROLE $FLOWMACHINE_FLOWDB_USER
                IN SCHEMA $schema
                GRANT ALL ON TABLES TO $FLOWMACHINE_FLOWDB_USER;

            GRANT ALL PRIVILEGES 
                ON ALL TABLES 
                IN SCHEMA $schema TO $FLOWMACHINE_FLOWDB_USER;

            GRANT USAGE
                ON SCHEMA $schema
                TO $FLOWMACHINE_FLOWDB_USER;

            GRANT ALL  
                ON ALL TABLES 
                IN SCHEMA $schema TO $FLOWMACHINE_FLOWDB_USER;
            
            GRANT CREATE
                ON SCHEMA $schema
                TO $FLOWMACHINE_FLOWDB_USER;
        COMMIT;
        "
done

declare -a schema_list_restricted=("events" "dfs" "infrastructure" "routing")
for schema in "${schema_list_restricted[@]}"
do
    echo "Restricting permissions to $FLOWMACHINE_FLOWDB_USER on $schema."
    psql --dbname="$POSTGRES_DB" -c "
        BEGIN;
            GRANT USAGE
                ON SCHEMA $schema
                TO $FLOWMACHINE_FLOWDB_USER;

            GRANT SELECT
                ON ALL TABLES 
                IN SCHEMA $schema TO $FLOWMACHINE_FLOWDB_USER;

            ALTER DEFAULT PRIVILEGES 
                IN SCHEMA $schema
                GRANT SELECT ON SEQUENCES TO $FLOWMACHINE_FLOWDB_USER;

            ALTER DEFAULT PRIVILEGES 
                IN SCHEMA $schema
                GRANT SELECT ON TABLES TO $FLOWMACHINE_FLOWDB_USER;
        END;
        "
done

echo "Give $FLOWMACHINE_FLOWDB_USER role read and update access to cache_touches sequence"
psql --dbname="$POSTGRES_DB" -c "
    BEGIN;
        GRANT USAGE, SELECT, UPDATE ON SEQUENCE cache.cache_touches TO $FLOWMACHINE_FLOWDB_USER;
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
        ALTER DEFAULT PRIVILEGES FOR ROLE $FLOWMACHINE_FLOWDB_USER
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
