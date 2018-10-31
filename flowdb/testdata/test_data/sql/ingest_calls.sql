/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

-- Create a partition for each day of data, there are only 7 altogether
-- so let's do them all by hand

BEGIN;
DELETE FROM events.calls;
CREATE TABLE IF NOT EXISTS events.calls_20160909 () INHERITS (events.calls);
CREATE TABLE IF NOT EXISTS events.calls_20160101 (
    CHECK ( datetime >= '20160101'::TIMESTAMPTZ
        AND datetime < '20160102'::TIMESTAMPTZ)
    ) INHERITS (events.calls);

COPY events.calls_20160101( datetime,msisdn_counterpart,id,msisdn,location_id,outgoing,duration, tac )
    FROM '/docker-entrypoint-initdb.d/data/records/calls/calls_20160101.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.calls_20160102 (
    CHECK ( datetime >= '20160102'::TIMESTAMPTZ
        AND datetime < '20160103'::TIMESTAMPTZ)
    ) INHERITS (events.calls);

COPY events.calls_20160102( datetime,msisdn_counterpart,id,msisdn,location_id,outgoing,duration, tac )
    FROM '/docker-entrypoint-initdb.d/data/records/calls/calls_20160102.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.calls_20160103 (
    CHECK ( datetime >= '20160103'::TIMESTAMPTZ
        AND datetime < '20160104'::TIMESTAMPTZ)
    ) INHERITS (events.calls);

COPY events.calls_20160103( datetime,msisdn_counterpart,id,msisdn,location_id,outgoing,duration, tac )
    FROM '/docker-entrypoint-initdb.d/data/records/calls/calls_20160103.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.calls_20160104 (
    CHECK ( datetime >= '20160104'::TIMESTAMPTZ
        AND datetime < '20160105'::TIMESTAMPTZ)
    ) INHERITS (events.calls);

COPY events.calls_20160104( datetime,msisdn_counterpart,id,msisdn,location_id,outgoing,duration, tac )
    FROM '/docker-entrypoint-initdb.d/data/records/calls/calls_20160104.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.calls_20160105 (
    CHECK ( datetime >= '20160105'::TIMESTAMPTZ
        AND datetime < '20160106'::TIMESTAMPTZ)
    ) INHERITS (events.calls);

COPY events.calls_20160105( datetime,msisdn_counterpart,id,msisdn,location_id,outgoing,duration, tac )
    FROM '/docker-entrypoint-initdb.d/data/records/calls/calls_20160105.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.calls_20160106 (
    CHECK ( datetime >= '20160106'::TIMESTAMPTZ
        AND datetime < '20160107'::TIMESTAMPTZ)
    ) INHERITS (events.calls);

COPY events.calls_20160106( datetime,msisdn_counterpart,id,msisdn,location_id,outgoing,duration, tac )
    FROM '/docker-entrypoint-initdb.d/data/records/calls/calls_20160106.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.calls_20160107 (
    CHECK ( datetime >= '20160107'::TIMESTAMPTZ
        AND datetime < '20160108'::TIMESTAMPTZ)
    ) INHERITS (events.calls);

COPY events.calls_20160107( datetime,msisdn_counterpart,id,msisdn,location_id,outgoing,duration, tac )
    FROM '/docker-entrypoint-initdb.d/data/records/calls/calls_20160107.csv'
    WITH DELIMITER ','
    CSV HEADER;

-- For testing purposes let's also add a very small table to the database, which can be
-- used for very quick tests, and for asserting that methods can point to alternative
-- tables
DROP TABLE IF EXISTS events.calls_short;
CREATE TABLE IF NOT EXISTS events.calls_short AS (SELECT * FROM events.calls LIMIT 20);

-- Add a self-call into the test data
INSERT INTO events.calls_20160101 (datetime, msisdn_counterpart, id, msisdn, location_id, outgoing, duration, tac) VALUES
('2016-01-01 00:00:06','self_caller','5ZNJA-PdRJ4-jxEdG-yOXpZ','self_caller','JxDglNVk',True,3393.0,92380772.0);
INSERT INTO events.calls_20160101 (datetime, msisdn_counterpart, id, msisdn, location_id, outgoing, duration, tac) VALUES
('2016-01-01 00:00:06','self_caller','5ZNJA-PdRJ4-jxEdG-yOXpZ','self_caller','JxDglNVk',False,3393.0,92380772.0);

CREATE INDEX ON events.calls_20160101 (msisdn);
CREATE INDEX ON events.calls_20160101 (msisdn_counterpart);
CREATE INDEX ON events.calls_20160101 (tac);
CREATE INDEX ON events.calls_20160101 (location_id);
CREATE INDEX ON events.calls_20160101 (datetime);
ANALYZE events.calls_20160101;
CREATE INDEX ON events.calls_20160102 (msisdn);
CREATE INDEX ON events.calls_20160102 (msisdn_counterpart);
CREATE INDEX ON events.calls_20160102 (tac);
CREATE INDEX ON events.calls_20160102 (location_id);
CREATE INDEX ON events.calls_20160102 (datetime);
ANALYZE events.calls_20160102;
CREATE INDEX ON events.calls_20160103 (msisdn);
CREATE INDEX ON events.calls_20160103 (msisdn_counterpart);
CREATE INDEX ON events.calls_20160103 (tac);
CREATE INDEX ON events.calls_20160103 (location_id);
CREATE INDEX ON events.calls_20160103 (datetime);
ANALYZE events.calls_20160103;
CREATE INDEX ON events.calls_20160104 (msisdn);
CREATE INDEX ON events.calls_20160104 (msisdn_counterpart);
CREATE INDEX ON events.calls_20160104 (tac);
CREATE INDEX ON events.calls_20160104 (location_id);
CREATE INDEX ON events.calls_20160104 (datetime);
ANALYZE events.calls_20160104;
CREATE INDEX ON events.calls_20160105 (msisdn);
CREATE INDEX ON events.calls_20160105 (msisdn_counterpart);
CREATE INDEX ON events.calls_20160105 (tac);
CREATE INDEX ON events.calls_20160105 (location_id);
CREATE INDEX ON events.calls_20160105 (datetime);
ANALYZE events.calls_20160105;
CREATE INDEX ON events.calls_20160106 (msisdn);
CREATE INDEX ON events.calls_20160106 (msisdn_counterpart);
CREATE INDEX ON events.calls_20160106 (tac);
CREATE INDEX ON events.calls_20160106 (location_id);
CREATE INDEX ON events.calls_20160106 (datetime);
ANALYZE events.calls_20160106;
CREATE INDEX ON events.calls_20160107 (msisdn);
CREATE INDEX ON events.calls_20160107 (msisdn_counterpart);
CREATE INDEX ON events.calls_20160107 (tac);
CREATE INDEX ON events.calls_20160107 (location_id);
CREATE INDEX ON events.calls_20160107 (datetime);
ANALYZE events.calls_20160107;
ANALYZE events.calls;

INSERT INTO available_tables (table_name, has_locations, has_subscribers, has_counterparts) VALUES ('calls', true, true, true)
    ON conflict (table_name)
    DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers, has_counterparts=EXCLUDED.has_counterparts;
COMMIT;