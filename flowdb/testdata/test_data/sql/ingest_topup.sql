/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

-- Create a partition for each day of data, there are only 7 altogether
-- so let's do them all by hand

BEGIN;
DELETE FROM events.topups;
CREATE TABLE IF NOT EXISTS events.topups_20160909 () INHERITS (events.topups);
CREATE TABLE IF NOT EXISTS events.topups_20160101 (
    CHECK ( datetime >= '20160101'::TIMESTAMPTZ
        AND datetime < '20160102'::TIMESTAMPTZ)
    ) INHERITS (events.topups);

COPY events.topups_20160101( id,msisdn,tac,datetime,location_id,recharge_amount,airtime_fee,tax_and_fee,pre_event_balance,post_event_balance )
    FROM '/docker-entrypoint-initdb.d/data/records/topups/topup_20160101.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.topups_20160102 (
    CHECK ( datetime >= '20160102'::TIMESTAMPTZ
        AND datetime < '20160103'::TIMESTAMPTZ)
    ) INHERITS (events.topups);

COPY events.topups_20160102( id,msisdn,tac,datetime,location_id,recharge_amount,airtime_fee,tax_and_fee,pre_event_balance,post_event_balance )
    FROM '/docker-entrypoint-initdb.d/data/records/topups/topup_20160102.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.topups_20160103 (
    CHECK ( datetime >= '20160103'::TIMESTAMPTZ
        AND datetime < '20160104'::TIMESTAMPTZ)
    ) INHERITS (events.topups);

COPY events.topups_20160103( id,msisdn,tac,datetime,location_id,recharge_amount,airtime_fee,tax_and_fee,pre_event_balance,post_event_balance )
    FROM '/docker-entrypoint-initdb.d/data/records/topups/topup_20160103.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.topups_20160104 (
    CHECK ( datetime >= '20160104'::TIMESTAMPTZ
        AND datetime < '20160105'::TIMESTAMPTZ)
    ) INHERITS (events.topups);

COPY events.topups_20160104( id,msisdn,tac,datetime,location_id,recharge_amount,airtime_fee,tax_and_fee,pre_event_balance,post_event_balance )
    FROM '/docker-entrypoint-initdb.d/data/records/topups/topup_20160104.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.topups_20160105 (
    CHECK ( datetime >= '20160105'::TIMESTAMPTZ
        AND datetime < '20160106'::TIMESTAMPTZ)
    ) INHERITS (events.topups);

COPY events.topups_20160105( id,msisdn,tac,datetime,location_id,recharge_amount,airtime_fee,tax_and_fee,pre_event_balance,post_event_balance )
    FROM '/docker-entrypoint-initdb.d/data/records/topups/topup_20160105.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.topups_20160106 (
    CHECK ( datetime >= '20160106'::TIMESTAMPTZ
        AND datetime < '20160107'::TIMESTAMPTZ)
    ) INHERITS (events.topups);

COPY events.topups_20160106( id,msisdn,tac,datetime,location_id,recharge_amount,airtime_fee,tax_and_fee,pre_event_balance,post_event_balance )
    FROM '/docker-entrypoint-initdb.d/data/records/topups/topup_20160106.csv'
    WITH DELIMITER ','
    CSV HEADER;

CREATE TABLE IF NOT EXISTS events.topups_20160107 (
    CHECK ( datetime >= '20160107'::TIMESTAMPTZ
        AND datetime < '20160108'::TIMESTAMPTZ)
    ) INHERITS (events.topups);

COPY events.topups_20160107( id,msisdn,tac,datetime,location_id,recharge_amount,airtime_fee,tax_and_fee,pre_event_balance,post_event_balance )
    FROM '/docker-entrypoint-initdb.d/data/records/topups/topup_20160107.csv'
    WITH DELIMITER ','
    CSV HEADER;

-- For testing purposes let's also add a very small table to the database, which can be
-- used for very quick tests, and for asserting that methods can point to alternative
-- tables
DROP TABLE IF EXISTS events.topups_short;
CREATE TABLE IF NOT EXISTS events.topups_short AS (SELECT * FROM events.topups LIMIT 20);

CREATE INDEX ON events.topups_20160101 (msisdn);
CREATE INDEX ON events.topups_20160101 (id);
CREATE INDEX ON events.topups_20160101 (tac);
CREATE INDEX ON events.topups_20160101 (location_id);
CREATE INDEX ON events.topups_20160101 (datetime);
ANALYZE events.topups_20160101;
CREATE INDEX ON events.topups_20160102 (msisdn);
CREATE INDEX ON events.topups_20160102 (id);
CREATE INDEX ON events.topups_20160102 (tac);
CREATE INDEX ON events.topups_20160102 (location_id);
CREATE INDEX ON events.topups_20160102 (datetime);
ANALYZE events.topups_20160102;
CREATE INDEX ON events.topups_20160103 (msisdn);
CREATE INDEX ON events.topups_20160103 (id);
CREATE INDEX ON events.topups_20160103 (tac);
CREATE INDEX ON events.topups_20160103 (location_id);
CREATE INDEX ON events.topups_20160103 (datetime);
ANALYZE events.topups_20160103;
CREATE INDEX ON events.topups_20160104 (msisdn);
CREATE INDEX ON events.topups_20160104 (id);
CREATE INDEX ON events.topups_20160104 (tac);
CREATE INDEX ON events.topups_20160104 (location_id);
CREATE INDEX ON events.topups_20160104 (datetime);
ANALYZE events.topups_20160104;
CREATE INDEX ON events.topups_20160105 (msisdn);
CREATE INDEX ON events.topups_20160105 (id);
CREATE INDEX ON events.topups_20160105 (tac);
CREATE INDEX ON events.topups_20160105 (location_id);
CREATE INDEX ON events.topups_20160105 (datetime);
ANALYZE events.topups_20160105;
CREATE INDEX ON events.topups_20160106 (msisdn);
CREATE INDEX ON events.topups_20160106 (id);
CREATE INDEX ON events.topups_20160106 (tac);
CREATE INDEX ON events.topups_20160106 (location_id);
CREATE INDEX ON events.topups_20160106 (datetime);
ANALYZE events.topups_20160106;
CREATE INDEX ON events.topups_20160107 (msisdn);
CREATE INDEX ON events.topups_20160107 (id);
CREATE INDEX ON events.topups_20160107 (tac);
CREATE INDEX ON events.topups_20160107 (location_id);
CREATE INDEX ON events.topups_20160107 (datetime);
ANALYZE events.topups_20160107;
ANALYZE events.topups;

INSERT INTO available_tables (table_name, has_locations, has_subscribers) VALUES ('topups', true, true)
    ON conflict (table_name)
    DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers;
COMMIT;
