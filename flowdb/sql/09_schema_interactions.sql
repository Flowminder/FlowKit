/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
Interaction -------------------------------------------------------

This schema contains tables related to interactions between 
subscribers.

  - subscriber:                 subscriber encountered in the event 
                                data.
  - date_dim:                   stores relevant information about a 
                                specific date.
  - time_dimension:             stores relevant information about a 
                                specific time.
  - locations:                  contains a new row for each time a cell
                                moves
  - receiving_parties:
  - event_supertable:           contains a row per call/mds/sms event. The
                                event_type field can be an integer for 
                                each type 1. calls, 2. sms, 3. mds and
                                fianlly 4. topup
  - subscriber_sightings:       contains a row per subscriber sighting event
  - calls:                      stores the additional call type data
  - sms:                        stores the additional sms type data
  - mds:                        stores the additional mobile data type data
  - topup:                      stores the additional topup type data
-----------------------------------------------------------
*/
CREATE SCHEMA IF NOT EXISTS interactions;

    CREATE TABLE IF NOT EXISTS interactions.subscriber(

        id                      BIGSERIAL PRIMARY KEY,
        msisdn                  TEXT,
        imei                    TEXT,
        imsi                    TEXT,
        tac                     BIGINT REFERENCES infrastructure.tacs(id)

        );

    CREATE INDEX ON interactions.subscriber (id);
    CREATE INDEX ON interactions.subscriber (id, msisdn);

    CREATE TABLE IF NOT EXISTS interactions.date_dim(

        date_sk                 SERIAL PRIMARY KEY,
        date                    DATE,
        day_of_week             TEXT,
        day_of_month            TEXT,
        year                    TEXT

        );

    CREATE TABLE IF NOT EXISTS interactions.time_dimension(

        time_sk                 SERIAL PRIMARY KEY,
        hour                    NUMERIC

        );

    CREATE TABLE IF NOT EXISTS interactions.locations(

        cell_id                 BIGSERIAL PRIMARY KEY,
        site_id                 TEXT,
        mno_cell_code           TEXT

        );

    SELECT AddGeometryColumn('interactions', 'locations', 'position', 4326, 'POINT', 2);

    CREATE INDEX IF NOT EXISTS interactions_locations_position_index
        ON interactions.locations
        USING GIST (position);

    CREATE TABLE IF NOT EXISTS interactions.receiving_parties(

        parties_key             BIGSERIAL PRIMARY KEY,
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        party_msisdn            TEXT

        );
    
    CREATE TABLE IF NOT EXISTS interactions.event_supertable (

        event_id                BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        time_sk                 BIGINT REFERENCES interactions.time_dimension(time_sk),
        date_sk                 BIGINT REFERENCES interactions.date_dim(date_sk),
        event_type              INTEGER,
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (event_id, date_sk)

    ) PARTITION BY LIST (date_sk);

    CREATE TABLE IF NOT EXISTS interactions.subscriber_sightings(

        sighting_id             BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        date_sk                 BIGINT REFERENCES interactions.date_dim(date_sk),
        time_sk                 BIGINT REFERENCES interactions.time_dimension(time_sk),
        event_super_table_id    BIGINT, /* REFERENCES interactions.event_supertable(event_id), - this can be added in PG12 */ 
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (sighting_id, date_sk)

    ) PARTITION BY LIST (date_sk);
    
    /* We need to an the CONSTRAINT for geography.geo_bridge here */
    ALTER TABLE geography.geo_bridge ADD CONSTRAINT locations_fkey FOREIGN KEY (cell_id) REFERENCES interactions.locations(cell_id);

    CREATE TABLE IF NOT EXISTS interactions.calls(

        super_table_id          BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        called_subscriber_id    BIGINT REFERENCES interactions.subscriber(id),
        calling_party_cell_id   BIGINT REFERENCES interactions.locations(cell_id),
        called_party_cell_id    BIGINT REFERENCES interactions.locations(cell_id),
        date_sk                 BIGINT REFERENCES interactions.date_dim(date_sk),
        time_sk                 BIGINT REFERENCES interactions.time_dimension(time_sk),
        calling_party_msisdn    TEXT,
        called_party_msisdn     TEXT,
        call_duration           NUMERIC,
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (super_table_id, date_sk)

    ) PARTITION BY LIST (date_sk);
    
    CREATE TABLE IF NOT EXISTS interactions.sms(

        super_table_id                BIGSERIAL,
        subscriber_id                 BIGINT REFERENCES interactions.subscriber(id),
        calling_party_cell_id         BIGINT REFERENCES interactions.locations(cell_id),
        parties_key                   BIGINT REFERENCES interactions.receiving_parties(parties_key),
        date_sk                       BIGINT REFERENCES interactions.date_dim(date_sk),
        time_sk                       BIGINT REFERENCES interactions.time_dimension(time_sk),
        calling_party_msisdn          TEXT,
        receiving_parties_msisdns     TEXT,
        timestamp                     TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (super_table_id, date_sk)

    ) PARTITION BY LIST (date_sk);
    
    CREATE TABLE IF NOT EXISTS interactions.mds(

        super_table_id          BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        date_sk                 BIGINT REFERENCES interactions.date_dim(date_sk),
        time_sk                 BIGINT REFERENCES interactions.time_dimension(time_sk),
        data_volume_total       NUMERIC,
        data_volume_up          NUMERIC,
        data_volume_down        NUMERIC,
        duration                NUMERIC,
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (super_table_id, date_sk)

    ) PARTITION BY LIST (date_sk);

    CREATE TABLE IF NOT EXISTS interactions.topup(

        super_table_id          BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        date_sk                 BIGINT REFERENCES interactions.date_dim(date_sk),
        time_sk                 BIGINT REFERENCES interactions.time_dimension(time_sk),
        type                    INTEGER,
        recharge_amount         NUMERIC,
        airtime_fee             NUMERIC,
        tax_and_fee             NUMERIC,
        pre_event_balance       NUMERIC,
        post_event_balance      NUMERIC,
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (super_table_id, date_sk)

    ) PARTITION BY LIST (date_sk);
