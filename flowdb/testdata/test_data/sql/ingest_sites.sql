/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

BEGIN;
DELETE FROM infrastructure.sites;

CREATE TEMP TABLE temp_sites (
    id TEXT,
    version NUMERIC,
    latitude NUMERIC,
    longitude NUMERIC,
    date_of_first_service TEXT,
    date_of_last_service TEXT
);

COPY temp_sites (
        id,
        version,
        latitude,
        longitude,
        date_of_first_service,
        date_of_last_service
    )
FROM
    '/docker-entrypoint-initdb.d/data/infrastructure/sites.csv'
WITH
    ( DELIMITER ',',
    HEADER true,
    FORMAT csv );

INSERT INTO infrastructure.sites (
	id, 
	version,
	date_of_first_service,
	date_of_last_service,
	geom_point
	)
    	SELECT
            id,
            version,
            date_of_first_service::date,
            date_of_last_service::date,
            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom_point
        FROM temp_sites;

COMMIT;