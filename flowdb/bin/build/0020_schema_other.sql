/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*

OTHER ----------------------------------------------------

Here we create schemas without tables. These are used
for reference by programs that connect to the database.

----------------------------------------------------------

*/


/*

Schema used for storing population estimates.

*/
CREATE SCHEMA IF NOT EXISTS population;

/*

Schema used for managing Digital Elevation Model (DEM)
datasets.

*/
CREATE SCHEMA IF NOT EXISTS elevation;

/*

Schema used by PGRouting for holding road networks.

*/

CREATE SCHEMA IF NOT EXISTS routing;

/*

Schema used for temp storage during etl.

*/

CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE etl.etl_records (
	id SERIAL NOT NULL,
	cdr_type VARCHAR,
	cdr_date DATE,
	state VARCHAR,
	timestamp TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id)
);

CREATE TABLE etl.post_etl_queries (
    id SERIAL NOT NULL,
    cdr_date DATE,
    cdr_type TEXT,
    type_of_query_or_check TEXT,
    outcome TEXT,
    optional_comment_or_description TEXT,
    timestamp TIMESTAMP WITH TIME ZONE
);

COMMENT ON TABLE etl.post_etl_queries
        IS 'Records outcomes of queries (e.g. simple quality checks) that are run '
           'as part of the regular ETL process, after data has been ingested.';

CREATE VIEW etl.ingest_state AS (
    SELECT DISTINCT ON (cdr_type, cdr_date)
           cdr_type, cdr_date, state, timestamp
    FROM etl.etl_records
    ORDER BY cdr_type, cdr_date, timestamp DESC
);

CREATE VIEW etl.deduped_post_etl_queries AS (
    SELECT DISTINCT ON (cdr_date, cdr_type, type_of_query_or_check)
           cdr_date, cdr_type, type_of_query_or_check, outcome, optional_comment_or_description, timestamp
    FROM etl.post_etl_queries
    INNER JOIN etl.ingest_state
    USING (cdr_date, cdr_type)
    WHERE state = 'ingested'
    ORDER BY cdr_date, cdr_type, type_of_query_or_check, timestamp DESC
);

