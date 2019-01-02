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

CREATE TYPE cdrtype AS ENUM ('voice', 'sms', 'mds');
CREATE TYPE status AS ENUM ('in_process', 'done', 'quarantine');
CREATE TABLE etl.etl (
	id SERIAL NOT NULL,
	file_name VARCHAR,
	cdr_type cdrtype,
	cdr_date DATE,
	status status,
	time_stamp TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id)
);
