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

Schema that stores calculated features (e.g. daily locations).

*/
CREATE SCHEMA IF NOT EXISTS features;

/*

The results schema is where Flowminder analysts store
the results of their analysis. This schema is designed to
have less privileges than other schemas. This is meant
to allow other applications (i.e. visualizations) to
have access to results data.

*/
CREATE SCHEMA IF NOT EXISTS results;


/*

Schema used for storing population estimates. See issue

	https://github.com/Flowminder/cdr-database/issues/86

for detailed discussion on what this schema addresses.

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

/*

Schema used for record keeping of aggregate calculations.

*/

CREATE SCHEMA IF NOT EXISTS aggregates;

CREATE TYPE aggstatus AS ENUM ('in_process', 'done', 'failed');
CREATE TABLE aggregates.aggregates (
	id SERIAL NOT NULL,
	aggregate_type VARCHAR,
	aggregate_date DATE,
	status aggstatus,
	cache_ref VARCHAR,
	time_stamp TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id)
);
