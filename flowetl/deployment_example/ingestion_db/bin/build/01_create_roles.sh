#!/bin/bash

export PGUSER="$POSTGRES_USER"

psql --dbname="$POSTGRES_DB" -c "REVOKE CONNECT ON DATABASE $POSTGRES_DB FROM PUBLIC;"

if [[ $INGESTION_DB_PASSWORD ]]
    then
        psql --dbname="$POSTGRES_DB" -c "CREATE ROLE $INGESTION_DB_USER WITH LOGIN PASSWORD '$INGESTION_DB_PASSWORD';"
    else
        echo "No password supplied for '$INGESTION_DB_USER' user: $INGESTION_DB_PASSWORD"
        exit 1
fi

psql --dbname="$POSTGRES_DB" -c "GRANT CONNECT ON DATABASE $POSTGRES_DB TO $INGESTION_DB_USER;"
psql --dbname="$POSTGRES_DB" -c "GRANT CREATE ON DATABASE $POSTGRES_DB TO $INGESTION_DB_USER;"

#
#  Adding permissions.
#
declare -a schema_list_permissive=("events" "reference")
for schema in "${schema_list_permissive[@]}"
do
    echo "Granting permissions to $INGESTION_DB_USER on $schema."
    psql --dbname="$POSTGRES_DB" -c "
        BEGIN;
            ALTER DEFAULT PRIVILEGES
                IN SCHEMA $schema
                GRANT ALL ON TABLES TO $INGESTION_DB_USER;
            ALTER DEFAULT PRIVILEGES FOR ROLE $INGESTION_DB_USER
                IN SCHEMA $schema
                GRANT ALL ON TABLES TO $INGESTION_DB_USER;
            GRANT ALL PRIVILEGES
                ON ALL TABLES
                IN SCHEMA $schema TO $INGESTION_DB_USER;
            GRANT USAGE
                ON SCHEMA $schema
                TO $INGESTION_DB_USER;
            GRANT ALL
                ON ALL TABLES
                IN SCHEMA $schema TO $INGESTION_DB_USER;

            GRANT CREATE
                ON SCHEMA $schema
                TO $INGESTION_DB_USER;
        COMMIT;
        "
done
