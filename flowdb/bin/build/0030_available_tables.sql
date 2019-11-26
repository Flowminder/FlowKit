/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
 AVAILABLE TABLES
 -------------

 Adds bookkeeping tables to track what is available for use by flowmachine, based
 on the root tables under the events schema.
 Should be updated when ingestion occurs.
*/

BEGIN;
    CREATE TABLE IF NOT EXISTS available_tables (
        table_name TEXT PRIMARY KEY,
        has_locations BOOL DEFAULT False,
        has_subscribers BOOL DEFAULT False,
        has_counterparts BOOL DEFAULT False
    );
    INSERT INTO available_tables (table_name)
        (SELECT tablename 
            FROM pg_tables 
               WHERE NOT EXISTS (
                SELECT c.relname AS child
                FROM
                    pg_inherits JOIN pg_class AS c ON (inhrelid=c.oid)
                    JOIN pg_class as p ON (inhparent=p.oid)
                    WHERE c.relname=tablename
                    ) AND
                schemaname='events')
          ON CONFLICT (table_name)
          DO NOTHING;
END;
