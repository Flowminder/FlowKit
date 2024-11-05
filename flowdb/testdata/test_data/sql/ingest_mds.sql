/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

-- Create a partition for each day of data, there are only 7 altogether
-- so let's do them all by hand

BEGIN;
DELETE FROM events.mds;
CREATE TABLE IF NOT EXISTS events.mds_20160909 PARTITION OF events.mds FOR VALUES FROM ('2016-09-09') TO ('2016-09-10');
CREATE TABLE IF NOT EXISTS events.mds_20160101 PARTITION OF events.mds FOR VALUES FROM ('2016-01-01') TO ('2016-01-02');

COPY events.mds_20160101( id,msisdn,tac,datetime,duration,location_id,volume_total,volume_upload,volume_download )
    FROM '/docker-entrypoint-initdb.d/data/records/mds/mds_20160101.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.mds_20160102 PARTITION OF events.mds FOR VALUES FROM ('2016-01-02') TO ('2016-01-03');

COPY events.mds_20160102( id,msisdn,tac,datetime,duration,location_id,volume_total,volume_upload,volume_download )
    FROM '/docker-entrypoint-initdb.d/data/records/mds/mds_20160102.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.mds_20160103 PARTITION OF events.mds FOR VALUES FROM ('2016-01-03') TO ('2016-01-04');

COPY events.mds_20160103( id,msisdn,tac,datetime,duration,location_id,volume_total,volume_upload,volume_download )
    FROM '/docker-entrypoint-initdb.d/data/records/mds/mds_20160103.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.mds_20160104 PARTITION OF events.mds FOR VALUES FROM ('2016-01-04') TO ('2016-01-05');

COPY events.mds_20160104( id,msisdn,tac,datetime,duration,location_id,volume_total,volume_upload,volume_download )
    FROM '/docker-entrypoint-initdb.d/data/records/mds/mds_20160104.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.mds_20160105 PARTITION OF events.mds FOR VALUES FROM ('2016-01-05') TO ('2016-01-06');

COPY events.mds_20160105( id,msisdn,tac,datetime,duration,location_id,volume_total,volume_upload,volume_download )
    FROM '/docker-entrypoint-initdb.d/data/records/mds/mds_20160105.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.mds_20160106 PARTITION OF events.mds FOR VALUES FROM ('2016-01-06') TO ('2016-01-07');

COPY events.mds_20160106( id,msisdn,tac,datetime,duration,location_id,volume_total,volume_upload,volume_download )
    FROM '/docker-entrypoint-initdb.d/data/records/mds/mds_20160106.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.mds_20160107 PARTITION OF events.mds FOR VALUES FROM ('2016-01-07') TO ('2016-01-08');

COPY events.mds_20160107( id,msisdn,tac,datetime,duration,location_id,volume_total,volume_upload,volume_download )
    FROM '/docker-entrypoint-initdb.d/data/records/mds/mds_20160107.csv'
    WITH DELIMITER ','
    CSV HEADER;

-- For testing purposes let's also add a very small table to the database, which can be
-- used for very quick tests, and for asserting that methods can point to alternative
-- tables
DROP TABLE IF EXISTS events.mds_short;
CREATE TABLE IF NOT EXISTS events.mds_short AS (SELECT * FROM events.mds LIMIT 20);

CREATE INDEX ON events.mds_20160101 (id);
CREATE INDEX ON events.mds_20160101 (msisdn);
CREATE INDEX ON events.mds_20160101 (tac);
CREATE INDEX ON events.mds_20160101 (location_id);
CREATE INDEX ON events.mds_20160101 (datetime);
ANALYZE events.mds_20160101;
CREATE INDEX ON events.mds_20160102 (msisdn);
CREATE INDEX ON events.mds_20160102 (id);
CREATE INDEX ON events.mds_20160102 (tac);
CREATE INDEX ON events.mds_20160102 (location_id);
CREATE INDEX ON events.mds_20160102 (datetime);
ANALYZE events.mds_20160102;
CREATE INDEX ON events.mds_20160103 (msisdn);
CREATE INDEX ON events.mds_20160103 (id);
CREATE INDEX ON events.mds_20160103 (tac);
CREATE INDEX ON events.mds_20160103 (location_id);
CREATE INDEX ON events.mds_20160103 (datetime);
ANALYZE events.mds_20160103;
CREATE INDEX ON events.mds_20160104 (msisdn);
CREATE INDEX ON events.mds_20160104 (id);
CREATE INDEX ON events.mds_20160104 (tac);
CREATE INDEX ON events.mds_20160104 (location_id);
CREATE INDEX ON events.mds_20160104 (datetime);
ANALYZE events.mds_20160104;
CREATE INDEX ON events.mds_20160105 (msisdn);
CREATE INDEX ON events.mds_20160105 (id);
CREATE INDEX ON events.mds_20160105 (tac);
CREATE INDEX ON events.mds_20160105 (location_id);
CREATE INDEX ON events.mds_20160105 (datetime);
ANALYZE events.mds_20160105;
CREATE INDEX ON events.mds_20160106 (msisdn);
CREATE INDEX ON events.mds_20160106 (id);
CREATE INDEX ON events.mds_20160106 (tac);
CREATE INDEX ON events.mds_20160106 (location_id);
CREATE INDEX ON events.mds_20160106 (datetime);
ANALYZE events.mds_20160106;
CREATE INDEX ON events.mds_20160107 (msisdn);
CREATE INDEX ON events.mds_20160107 (id);
CREATE INDEX ON events.mds_20160107 (tac);
CREATE INDEX ON events.mds_20160107 (location_id);
CREATE INDEX ON events.mds_20160107 (datetime);
ANALYZE events.mds_20160107;
ANALYZE events.mds;

INSERT INTO available_tables (table_name, has_locations, has_subscribers) VALUES ('mds', true, true)
    ON conflict (table_name)
    DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers;
COMMIT;

INSERT INTO etl.etl_records (cdr_type, cdr_date, state, timestamp) VALUES
    ('mds', '2016-01-01'::DATE, 'ingested', NOW()),
    ('mds', '2016-01-02'::DATE, 'ingested', NOW()),
    ('mds', '2016-01-03'::DATE, 'ingested', NOW()),
    ('mds', '2016-01-04'::DATE, 'ingested', NOW()),
    ('mds', '2016-01-05'::DATE, 'ingested', NOW()),
    ('mds', '2016-01-06'::DATE, 'ingested', NOW()),
    ('mds', '2016-01-07'::DATE, 'ingested', NOW());